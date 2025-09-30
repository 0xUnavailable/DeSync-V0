package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

type FileInfo struct {
	RelPath string
	Content string
	Tokens  int
}

func main() {
	root := flag.String("root", ".", "Root directory to scan (default: current dir)")
	output := flag.String("output", "", "Output file base name (default: codebase-bundle; for splits: {base}-1.txt, etc.)")
	showTokens := flag.Bool("tokens", false, "Show token counts for each file")
	splitParts := flag.Int("split", 0, "Number of parts to split output into (0=auto based on -max-tokens; requires -output for >1)")
	maxTokensPerFile := flag.Int("max-tokens", 50000, "Max tokens per output file (used for auto-split when -split=0)")
	flag.Parse()

	files := flag.Args() // Remaining args are specific filenames (relative to root)

	if *splitParts > 1 && *output == "" {
		fmt.Println("Error: -split >1 requires -output to be specified for multi-file output.")
		os.Exit(1)
	}

	if *splitParts < 0 {
		fmt.Println("Error: -split must be >= 0")
		os.Exit(1)
	}

	if *maxTokensPerFile <= 0 {
		fmt.Println("Error: -max-tokens must be > 0")
		os.Exit(1)
	}

	// Default output if empty
	if *output == "" {
		*output = "codebase-bundle"
	}

	var allFiles []FileInfo

	if len(files) > 0 {
		// Read specific files
		for _, relPath := range files {
			if filepath.Base(relPath) == "bundle.go" {
				continue // Skip bundle.go
			}
			fullPath := filepath.Join(*root, relPath)
			info := readFileInfo(fullPath, relPath)
			if info.RelPath != "" { // Valid file was read
				allFiles = append(allFiles, info)
			}
		}
	} else {
		// Walk entire codebase
		err := filepath.WalkDir(*root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				// Skip common ignore dirs
				name := d.Name()
				if name == "vendor" || name == ".git" || name == "node_modules" {
					return filepath.SkipDir
				}
				return nil
			}
			// Filter for relevant files (Go-focused, but includes common config/docs)
			if filepath.Base(path) == "bundle.go" || filepath.Base(path) == "desync_test.go" {
				return nil // Skip bundle.go and desync_test.go
			}
			ext := filepath.Ext(path)
			if ext == ".go" || ext == ".html" {
				relPath, _ := filepath.Rel(*root, path)
				info := readFileInfo(path, relPath)
				if info.RelPath != "" { // Valid file was read
					allFiles = append(allFiles, info)
				}
			}
			return nil
		})
		if err != nil {
			fmt.Printf("Error walking directory: %v\n", err)
			os.Exit(1)
		}
	}

	if len(allFiles) == 0 {
		fmt.Println("No files found.")
		os.Exit(0)
	}

	// Sort files by token count descending for better balancing
	sort.Slice(allFiles, func(i, j int) bool {
		return allFiles[i].Tokens > allFiles[j].Tokens
	})

	totalTokens := sumTokens(allFiles)
	numParts := *splitParts

	// Auto-split if split=0
	if numParts == 0 {
		if totalTokens <= *maxTokensPerFile {
			numParts = 1
		} else {
			numParts = int(math.Ceil(float64(totalTokens) / float64(*maxTokensPerFile)))
			// Cap at reasonable number, e.g., 2-3 as per user preference
			if numParts > 3 {
				numParts = 3
			}
		}
	}

	if numParts == 1 {
		// Single output
		outputFile := *output
		if !strings.HasSuffix(outputFile, ".txt") {
			outputFile += ".txt"
		}
		outputSingle(allFiles, outputFile, *showTokens)
		fmt.Printf("Generated single file: %s (files: %d, tokens: %d)\n", outputFile, len(allFiles), totalTokens)
	} else {
		// Split into parts
		parts := splitFilesIntoParts(allFiles, numParts, *maxTokensPerFile)
		for i, partFiles := range parts {
			if len(partFiles) == 0 {
				continue // Skip empty parts
			}
			partNum := i + 1
			partOutput := generatePartFilename(*output, partNum)
			outputSingle(partFiles, partOutput, *showTokens)
			fmt.Printf("Generated part %d/%d: %s (files: %d, tokens: %d)\n", 
				partNum, len(parts), partOutput, len(partFiles), sumTokens(partFiles))
		}
		
		// Print overall summary
		fmt.Printf("\n--- OVERALL SUMMARY ---\n")
		fmt.Printf("Total files: %d\n", len(allFiles))
		fmt.Printf("Total tokens: %d\n", totalTokens)
		fmt.Printf("Split into: %d parts (target ~%d tokens/file)\n", numParts, *maxTokensPerFile)
	}
}

func readFileInfo(fullPath, relPath string) FileInfo {
	contentBytes, err := os.ReadFile(fullPath)
	if err != nil {
		fmt.Printf("Warning: Could not read file %s: %v\n", relPath, err)
		return FileInfo{} // Return empty FileInfo to indicate error
	}
	content := string(contentBytes)
	tokens := estimateTokens(content)
	return FileInfo{
		RelPath: relPath,
		Content: content,
		Tokens:  tokens,
	}
}

func generatePartFilename(baseOutput string, partNum int) string {
	// Handle file extension
	if strings.HasSuffix(baseOutput, ".txt") {
		base := strings.TrimSuffix(baseOutput, ".txt")
		return fmt.Sprintf("%s-%d.txt", base, partNum)
	} else {
		return fmt.Sprintf("%s-%d.txt", baseOutput, partNum)
	}
}

func outputSingle(files []FileInfo, outputFile string, showTokens bool) {
	var print func(string, ...any)
	var writer *os.File
	
	var err error
	writer, err = os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file %s: %v\n", outputFile, err)
		os.Exit(1)
	}
	defer writer.Close()
	print = func(format string, args ...any) {
		fmt.Fprintf(writer, format, args...)
	}

	for _, f := range files {
		safeContent := strings.ReplaceAll(f.Content, "---", "- - -")
		if showTokens {
			print("--- FILE: %s (tokens: %d) ---\n%s\n\n", f.RelPath, f.Tokens, safeContent)
		} else {
			print("--- FILE: %s ---\n%s\n\n", f.RelPath, safeContent)
		}
	}

	// Print summary if tokens shown
	if showTokens {
		totalTokens := sumTokens(files)
		numFiles := len(files)
		print("\n--- SUMMARY ---\n")
		print("Total files: %d\n", numFiles)
		print("Total tokens: %d\n", totalTokens)
		if numFiles > 0 {
			print("Average tokens per file: %.1f\n", float64(totalTokens)/float64(numFiles))
		}
	}
}

func splitFilesIntoParts(allFiles []FileInfo, numParts int, maxTokens int) [][]FileInfo {
	if numParts <= 1 || len(allFiles) == 0 {
		return [][]FileInfo{allFiles}
	}

	// Initialize parts
	parts := make([][]FileInfo, numParts)
	partTokens := make([]int, numParts)

	// Use a greedy algorithm: assign each file to the part with the fewest tokens
	for _, file := range allFiles {
		// Find the part with minimum tokens that won't exceed max if possible
		minPart := 0
		minTokens := partTokens[0]
		for i := 1; i < numParts; i++ {
			if partTokens[i] < minTokens {
				minTokens = partTokens[i]
				minPart = i
			}
		}
		
		// If adding would exceed max and there's another part under, prefer it
		if partTokens[minPart]+file.Tokens > maxTokens {
			for i := 0; i < numParts; i++ {
				if i != minPart && partTokens[i]+file.Tokens <= maxTokens {
					minPart = i
					break
				}
			}
		}
		
		// Add file to the selected part
		parts[minPart] = append(parts[minPart], file)
		partTokens[minPart] += file.Tokens
	}

	return parts
}

func sumTokens(files []FileInfo) int {
	total := 0
	for _, f := range files {
		total += f.Tokens
	}
	return total
}

// estimateTokens provides a rough approximation of token count
// This is a simplified version - real tokenizers are more complex
func estimateTokens(content string) int {
	if len(content) == 0 {
		return 0
	}

	// Split on whitespace and common delimiters for word counting
	words := 0
	inWord := false
	
	for _, r := range content {
		if unicode.IsSpace(r) || unicode.IsPunct(r) {
			if inWord {
				words++
				inWord = false
			}
		} else {
			inWord = true
		}
	}
	
	// Count the last word if content doesn't end with delimiter
	if inWord {
		words++
	}
	
	// Rough approximation: tokens ≈ words * 1.3
	// This accounts for subword tokenization in modern tokenizers
	tokens := int(float64(words) * 1.3)
	
	// Fallback to character-based if word count seems too low
	charBasedTokens := len(content) / 4
	if charBasedTokens > tokens {
		tokens = charBasedTokens
	}
	
	// Minimum of 1 token for non-empty content
	if tokens == 0 && len(content) > 0 {
		tokens = 1
	}
	
	return tokens
}

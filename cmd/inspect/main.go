package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	blocksDir := flag.String("dir", "blocks", "blocks directory")
	showBlock := flag.Int("block", -1, "show full block by height (default: -1 for index only)")
	flag.Parse()

	indexFile := filepath.Join(*blocksDir, "index.json")
	data, err := os.ReadFile(indexFile)
	if err != nil {
		log.Fatalf("failed to read index.json: %v", err)
	}

	var index []map[string]interface{}
	if err := json.Unmarshal(data, &index); err != nil {
		log.Fatalf("failed to parse index.json: %v", err)
	}

	// If only index requested
	if *showBlock == -1 {
		fmt.Printf("=== Block Index (%d blocks) ===\n", len(index))
		for _, entry := range index {
			fmt.Printf("Height=%v Txs=%v File=%v Time=%v\n",
				entry["height"], entry["txs"], entry["filename"], entry["time"])
		}
		return
	}

	// Show a specific block
	filename := ""
	for _, entry := range index {
		if int(entry["height"].(float64)) == *showBlock {
			filename = entry["filename"].(string)
			break
		}
	}
	if filename == "" {
		log.Fatalf("block %d not found in index", *showBlock)
	}

	fullPath := filepath.Join(*blocksDir, filename)
	blockData, err := os.ReadFile(fullPath)
	if err != nil {
		log.Fatalf("failed to read block file: %v", err)
	}

	fmt.Printf("=== Block %d ===\n", *showBlock)
	fmt.Println(string(blockData))
}

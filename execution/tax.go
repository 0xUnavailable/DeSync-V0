package execution

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type TaxEvent struct {
	Player string `json:"player"`
	Plot   string `json:"plot"`
	Amount int    `json:"taxPaid"`
}

var (
	monthEvents []TaxEvent
	monthMu     sync.Mutex
	monthNumber = 1
)

func AddTaxEvent(player, plot string, amount int) {
	monthMu.Lock()
	defer monthMu.Unlock()
	monthEvents = append(monthEvents, TaxEvent{Player: player, Plot: plot, Amount: amount})
}

func FlushMonth(dir string) {
	monthMu.Lock()
	defer monthMu.Unlock()

	if len(monthEvents) == 0 {
		return
	}

	_ = os.MkdirAll(dir, 0755)
	filename := filepath.Join(dir, "month-"+strconv.Itoa(monthNumber)+".json")
	data, _ := json.MarshalIndent(map[string]interface{}{
		"month":  monthNumber,
		"events": monthEvents,
	}, "", "  ")
	_ = os.WriteFile(filename, data, 0644)

	// reset for next month
	monthEvents = nil
	monthNumber++
}

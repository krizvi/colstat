package main

import (
	"flag"
	"fmt"
	"github.com/krizvi/colstat/codebase/cli/engine"
	"github.com/krizvi/colstat/codebase/shared"
	"io"
	"os"
	"sync"
)

// main The program will receive two optional input parameters each with a default value:
//
//	-col: The column on which to execute the operation. It defaults to 1.
//	-op: The operation to execute on the selected column. Initially, this tool will
//
//	support two operations: sum, which calculates the sum of all values in the column,
//	and avg, which determines the average value of the column. You can add more
//	operations later if you want.
//
// 	In addition to the two optional flags, this tool accepts any number of file names to
// 	process. If the user provides more than one file name, the tool combines the results
// 	for the same column in all files.

func main() {
	// Verify and parse arguments
	op := flag.String("op", "sum", "Operation to be executed")
	column := flag.Int("col", 1, "CSV column to be extracted")
	delimiter := flag.String("delimiter", ",", "CSV delimiter to be used")
	flag.Parse()

	if err := run(flag.Args(), *op, *column, []rune(*delimiter)[0], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

}

var opFunc engine.StatsFunc

func run(filenames []string, op string, col int, delimiter rune, outfile io.Writer) error {
	if len(filenames) == 0 {
		return shared.ErrNoFiles
	}
	if col < 1 {
		return shared.ErrInvalidColumn
	}
	switch op {
	case "sum":
		opFunc = engine.Sum
	case "avg":
		opFunc = engine.Avg
	default:
		return fmt.Errorf("%w: %s is not supported", shared.ErrInvalidOperation, op)
	}

	consolidate := make([]float64, 0)

	// create the channels
	resCh := make(chan []float64)
	errCh := make(chan error)
	doneCh := make(chan struct{})

	wg := sync.WaitGroup{}

	// Loop through all files adding their data to consolidate
	for _, fname := range filenames {
		wg.Add(1)
		go processFile(&wg, fname, col, delimiter, resCh, errCh, doneCh)
	}

	go func() {
		wg.Wait()
		close(doneCh)
	}()

	for {
		select {
		case err := <-errCh:
			return err
		case data := <-resCh:
			// Append the data to consolidate
			consolidate = append(consolidate, data...)
		case <-doneCh:
			_, err := fmt.Fprintf(outfile, "%s of %d recs => %.2f\n", op, len(consolidate), opFunc(consolidate))
			return err
		}
	}
}

func processFile(wg *sync.WaitGroup, fname string, col int, delimiter rune, resch chan []float64, errCh chan error, doneCh chan struct{}) {
	defer wg.Done()

	// Open the file for reading
	f, err := os.Open(fname)
	if err != nil {
		errCh <- fmt.Errorf("Cannot open file: %w", err)
		return
	}

	//fmt.Println("Processsing:", fname)
	// Parse the CSV into a slice of float64 numbers
	data, err := engine.CSV2Float(f, col, delimiter)
	if err != nil {
		errCh <- err
		return
	}

	if err := f.Close(); err != nil {
		errCh <- err
		return
	}

	resch <- data
}

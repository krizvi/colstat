package engine

import (
	"encoding/csv"
	"fmt"
	"github.com/krizvi/colstat/codebase/shared"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func Sum(data []float64) float64 {
	sum := 0.0

	for _, v := range data {
		sum += v
	}

	return sum
}
func Avg(data []float64) float64 {
	return Sum(data) / float64(len(data))
}

// StatsFunc defines a generic statistical function
type StatsFunc func(data []float64) float64

func CreateRandomCSVData(path string) error {
	now := time.Now().UnixNano()

	// Create a new CSV file
	file, err := os.Create(fmt.Sprintf("%s/data%d.csv", path, now))
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)

	// Write the header row
	writer.Write([]string{"Column 1", "Column 2", "Column 3", "Column 4", "Column 5", "Column 6", "Column 7", "Column 8", "Column 9", "Column 10", "Column 11", "Column 12"})

	// Write the data rows
	for i := 0; i < 100000; i++ { // Change the number of rows as per your requirement
		nanoNow := int64(time.Now().Nanosecond())
		rand.Seed(nanoNow)
		row := []string{}
		for j := 0; j < 12; j++ { // Change the number of columns as per your requirement
			var value string
			switch {
			case j < 8: // 70% of columns should be float data
				value = strconv.FormatFloat(rand.Float64()*float64(nanoNow), 'f', 4, 64)
			case j < 9: // 10% of columns should be string data
				value = "string" + strconv.Itoa(i)
			case j < 10: // 10% of columns should be date data
				value = time.Now().AddDate(0, 0, -rand.Intn(365)).Format("2006-01-02")
			default: // Rest of the columns should be boolean data
				value = strconv.FormatBool(rand.Intn(2) == 1)
			}
			row = append(row, value)
		}
		writer.Write(row)
	}

	writer.Flush()

	return nil
}
func CSV2Float(r io.Reader, column int, delimiter rune) ([]float64, error) {
	// Create the CSV Reader used to read in data from CSV files
	csvReader := csv.NewReader(r)
	csvReader.Comma = delimiter
	csvReader.ReuseRecord = true

	// adjust the start for 0 based index
	column--

	// Read in all CSV data
	//allData, err := csvReader.ReadAll()
	//if err != nil {
	//	return nil, fmt.Errorf("Cannot read data from file: %w", err)
	//}

	//fmt.Println("total Records:", len(allData))

	// loop through all records
	var data []float64

	for i := 0; ; i++ {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Something went wrong: %w", err)
		}

		// skip the header
		if i == 0 {
			continue
		}

		// checking  number of columns in the CSV file
		if len(row) <= column {
			// file does not have enough columns
			return nil, fmt.Errorf("%w: File has only %d columns", shared.ErrInvalidColumn, len(row))
		}

		// 	Finally, try to convert the value of the given column to a float64 by using the function ParseFloat of
		//	the strconv package. If the conversion fails, return an error wrapping ErrNotNumber. Otherwise,
		//	append the value to the data slice.
		if v, err := strconv.ParseFloat(row[column], 64); err != nil {
			return nil, fmt.Errorf("%w: %s is not a valid number", shared.ErrNotNumber, row[column])
		} else {
			data = append(data, v)
		}
	}

	return data, nil

}

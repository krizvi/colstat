package main

import (
	"bytes"
	"fmt"
	"github.com/krizvi/colstat/codebase/cli/engine"
	"github.com/krizvi/colstat/codebase/shared"
	"github.com/stretchr/testify/require"
	"io"
	"log"
	"path/filepath"
	"testing"
)

//func TestCreateRandomData(t *testing.T) {
//	cwd, err := os.Getwd()
//	require.NoErrorf(t, err, "err")
//	log.Println("Working directory", cwd)
//	for i := 0; i < 10; i++ {
//		require.NoError(t, engine.CreateRandomCSVData("../../resources"), "Error found")
//	}
//}

// TestOperations will test the operations and compare their results
func TestOperations(t *testing.T) {
	// Create a data variable to hold the input data for the tests as a slice of slices
	// of floating point numbers:
	data := [][]float64{
		{10, 20, 15, 30, 45, 50, 100, 30},
		{5.5, 8, 2.2, 9.75, 8.45, 3, 2.5, 10.25, 4.75, 6.1, 7.67, 12.287, 5.47},
		{-10, -20},
		{102, 37, 44, 57, 67, 129},
	}

	// Next, define the test cases by using the table-driven testing concept. Each test
	// case has a name, the operation function to execute, and the expected results
	// Test cases for Operations Test
	testCases := []struct {
		name string
		op   engine.StatsFunc
		exp  []float64
	}{
		{"Sum", engine.Sum, []float64{300, 85.927, -30, 436}},
		{"Avg", engine.Avg, []float64{37.5, 6.609769230769231, -15, 72.666666666666666}},
	}

	for _, tstcase := range testCases {
		for d, expResult := range tstcase.exp {
			testName := fmt.Sprintf("%sRun%d", tstcase.name, d)

			passed := t.Run(testName, func(t *testing.T) {
				computeResult := tstcase.op(data[d])
				log.Println(testName, "result:", computeResult, " expResult:", expResult)
				require.Equal(t, computeResult, expResult)
			})
			require.True(t, passed)

		}
	}
}

func TestCSV2FloatArray(t *testing.T) {
	data := `
name, subject, score,s2
khalid,physics,29,37.22
khalid1,physics,35,12.44
khalid1,physics,35,435.44
khalid1,physics,35,12.33
khalid1,physics,35,89.0
khalid1,physics,35,989.234
khalid1,physics,35,123.37
khalid1,physics,35,37.37
khalid1,physics,35,21.22
khalid1,physics,35,22.33
khalid1,physics,35,1
khalid1,physics,33,987.234
`
	_, err := engine.CSV2Float(bytes.NewBufferString(data), 3, ',')
	require.NoErrorf(t, err, "error found")

	testCases := []struct {
		name         string
		col          int
		csv2floatErr error
		data         io.Reader
		expResult    []float64
		op           engine.StatsFunc
		opResult     float64
	}{
		{"CSV2Float", 3, nil, bytes.NewBufferString(data), []float64{29, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 33}, engine.Sum, 412},
		{"CSV2Float", 1, shared.ErrNotNumber, bytes.NewBufferString(data), nil, nil, 0},
		{"CSV2Float", 2, shared.ErrNotNumber, bytes.NewBufferString(data), nil, nil, 0},
		{"CSV2Float", 4, nil, bytes.NewBufferString(data), []float64{37.22, 12.44, 435.44, 12.33, 89.0, 989.234, 123.37, 37.37, 21.22, 22.33, 1.0, 987.234}, engine.Avg, 230.68233333333333},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.CSV2Float(bytes.NewBufferString(data), tc.col, ',')
			require.ErrorIs(t, err, tc.csv2floatErr)
			require.Equal(t, result, tc.expResult)

			if tc.op != nil {
				opRes := tc.op(result)
				require.Equal(t, opRes, tc.opResult)
			}

		})
	}

}

func BenchmarkRun(b *testing.B){
	filenames,err:=filepath.Glob("../../resources/data*.csv")
	if err!=nil{
		b.Fatal(err)
	}
	
	b.ResetTimer()
	
	for i:=0;i<b.N;i++{
		if err:=run(filenames, "avg", 2, ',', io.Discard);err!=nil{
			b.Error(err)
		}
	}

}
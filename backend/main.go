package main

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/arrow/table"
)

func main() {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "intField", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "stringField", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "floatField", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1, 0, 3, 0, 5}, []bool{true, false, true, false, true})

	rec := builder.NewRecord()
	defer rec.Release()

	tbl := array.NewTableFromRecords(schema, []array.Record{rec})
	defer tbl.Release()

	ipcWriter := table.NewFileWriter(memory.DefaultAllocator, tbl.Schema())
	defer ipcWriter.Close()
	ipcWriter.Write(tbl)

	// Save IPC data to a file
	file, err := os.WriteFile("table_data.arrow", ipcWriter.Finish(), 0644)
	if err != nil {
		fmt.Println("Error writing file:", err)
		return
	}
	fmt.Println("Table data saved to file: table_data.arrow")
}

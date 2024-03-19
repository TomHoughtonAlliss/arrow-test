package main

import (
	"log"
	"os"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func main() {
	// Allocate memory
	pool := memory.NewGoAllocator()

	// Define Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "Ints", Type: arrow.PrimitiveTypes.Int32},
			{Name: "Floats", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Create record builder b using memory pool and schema
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	// Add some values to fields
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

	// Data entry as a new record
	rec1 := b.NewRecord()
	defer rec1.Release()

	// Add some more values to fields
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)

	// Data entry as a new record
	rec2 := b.NewRecord()
	defer rec2.Release()

	// Create table from schema and records
	tbl := array.NewTableFromRecords(schema, []array.Record{rec1, rec2})
	defer tbl.Release()

	// Open a file to write in IPC format
	file, err := os.Create("output.ipc")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create IPC writer
	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema))
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	// Iterate over record batches in the table and write each batch to the IPC file
	iter, err := array.NewRecordReader(tbl.Schema(), []array.Record{rec1, rec2})
	for iter.Next() {
		rec := iter.Record()
		err := writer.Write(rec)
		if err != nil {
			log.Fatal(err)
		}
	}
	if err != nil {
		log.Fatal(err)
	}

}

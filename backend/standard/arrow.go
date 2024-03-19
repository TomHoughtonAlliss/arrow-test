package standard

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func convertToIPC(record array.Record) []byte {

	buf := new(bytes.Buffer)

	writer := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))

	err := writer.Write(record)
	if err != nil {
		panic(err)
	}

	err = writer.Close()
	if err != nil {
		panic(err)
	}

	ipcData := buf.Bytes()

	return ipcData

}

func CreateTable() {

	pool := memory.NewGoAllocator()

	// create schema - each field is a column in our table
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "field one", Type: arrow.PrimitiveTypes.Int64},
		{Name: "field two", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// create each column as arrays - data here is arbitrary (must be the same length)
	fieldOneBuilder := array.NewInt64Builder(pool)
	fieldTwoBuilder := array.NewInt64Builder(pool)

	fieldOneBuilder.AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)
	fieldTwoBuilder.AppendValues([]int64{10, 20, 30, 40, 50, 60}, nil)

	fieldOneColumn := fieldOneBuilder.NewArray()
	defer fieldOneColumn.Release()

	fieldTwoColumn := fieldTwoBuilder.NewArray()
	defer fieldTwoColumn.Release()

	// convert arrays to chunked arrays
	chunkedFieldOne := array.NewChunked(arrow.PrimitiveTypes.Int64, []array.Interface{fieldOneColumn})
	chunkedFieldTwo := array.NewChunked(arrow.PrimitiveTypes.Int64, []array.Interface{fieldTwoColumn})

	// convert chunked arrays to columns
	columns := make([]array.Column, 2)

	columnOne := array.NewColumn(schema.Field(0), chunkedFieldOne)
	columnTwo := array.NewColumn(schema.Field(1), chunkedFieldTwo)

	columns[0] = *columnOne
	columns[1] = *columnTwo

	// convert schema and columns into a table. -1 here means we don't know how many rows we want yet
	table := array.NewTable(schema, columns, -1)
	defer table.Release()

	fmt.Print(table.Column(1).Data().Chunk(0))

}

func CreateRecord() {
	pool := memory.NewGoAllocator()

	// create schema - each field is a column in our table
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "field one", Type: arrow.PrimitiveTypes.Int64},
		{Name: "field two", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// create each column as arrays - data here is arbitrary (must be the same length)
	fieldOneBuilder := array.NewInt64Builder(pool)
	fieldTwoBuilder := array.NewInt64Builder(pool)

	fieldOneBuilder.AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)
	fieldTwoBuilder.AppendValues([]int64{10, 20, 30, 40, 50, 60}, nil)

	fieldOneColumn := fieldOneBuilder.NewArray()
	defer fieldOneColumn.Release()

	fieldTwoColumn := fieldTwoBuilder.NewArray()
	defer fieldTwoColumn.Release()

	columns := []array.Interface{
		fieldOneColumn,
		fieldTwoColumn,
	}

	record := array.NewRecord(
		schema,
		columns,
		-1,
	)

	fmt.Println(record)

	ipcFormat := convertToIPC(record)
	fmt.Println(ipcFormat)

}

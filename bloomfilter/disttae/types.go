package disttae

import (
	"sds_use/bloomfilter/disttae/logtailreplay"
	"sds_use/bloomfilter/fileservice"
	"sds_use/bloomfilter/txn/client"
)

// txnDatabase represents an opened database in a transaction
type txnDatabase struct {
	databaseId        uint64
	databaseName      string
	databaseType      string
	databaseCreateSql string
	txn               *Transaction
}

// Transaction represents a transaction
type Transaction struct {
	op     client.TxnOperator
	engine *Engine
}

type Engine struct {
	fs fileservice.FileService
}

func (e *Engine) getPartition(databaseId, tableId uint64) *logtailreplay.Partition {

}

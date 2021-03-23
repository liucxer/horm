package horm

import "strings"

type ExecSql interface {
	Exec() (*SqliteExecResult, error)
}

type QueryRow interface {
	QueryRowInto(object interface{}) error
}

type Query interface {
	QueryInto(object interface{}) error
}

type dropTable struct {
	TableName string
	DB
}

func DropTable(TableName string) *dropTable {
	return &dropTable{TableName: TableName}
}

func (d *dropTable) WithDB(db DB) *dropTable {
	d.DB = db
	return d
}

func (d *dropTable) Exec() (*SqliteExecResult, error) {
	return d.DB.Exec("DROP TABLE IF EXISTS " + strings.ToUpper(d.TableName))
}

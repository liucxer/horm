package horm_test

import (
	"fmt"
	"testing"

	"github.com/liucxer/horm"
)

type sqliteMaster struct {
	SType    string `json:"type" orm:"type"`
	Name     string `json:"name" orm:"name"`
	TblName  string `json:"tbl_name" orm:"tbl_name"`
	RootPage int    `json:"rootpage" orm:"rootpage"`
	Sql      string `json:"sql" orm:"sql"`
}

func (u sqliteMaster) TableName() string {
	return "sqlite_master"
}

func TestDBModelToFields(t *testing.T) {
	fields, err := horm.DBModelToFields(&sqliteMaster{
		SType: "a",
		Name:  "vvvv",
	})
	if err != nil {
		return
	}
	fmt.Println(fields)
}

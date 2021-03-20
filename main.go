package main

import (
	"github.com/liucxer/hlog"
	_ "github.com/mattn/go-sqlite3"
)

type sqliteMaster struct {
	SType    string `json:"type"`
	Name     string `json:"name"`
	TblName  string `json:"tbl_name"`
	RootPage int    `json:"rootpage"`
	Sql      string `json:"sql"`
}

// 查看当前表
func ShowTables() error {
	db, err := NewSqliteDB("D:\\sqlite\\orm.db")
	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	rows, err := db.Query("select * from sqlite_master")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for {
		if !rows.Next() {
			break
		}
		t := sqliteMaster{}
		SType := ""
		Name := ""
		TblName := ""
		RootPage := 0
		Sql := ""

		err = rows.Scan(&SType, &Name, &TblName, &RootPage, &Sql)
		if err != nil {
			hlog.Error("rows.Scan err:%v", err)
			return err
		}
		t.SType = SType
		t.Name = Name
		t.TblName = TblName
		t.RootPage = RootPage
		t.Sql = Sql
		hlog.Info("sqliteMaster:%+v", t)
	}

	return nil
}

func UserTable() error {
	db, err := NewSqliteDB("gee.db")
	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	// 删除User表
	_, err = db.Exec("DROP TABLE IF EXISTS USER")
	if err != nil {
		return err
	}

	// 创建User表
	_, err = db.Exec("CREATE TABLE USER (NAME VARCHAR(255))")
	if err != nil {
		return err
	}

	// 往User表写一行数据
	_, err = db.Exec("INSERT INTO USER (NAME) VALUES (?), (?)", "liucx", "wangli")
	if err != nil {
		return err
	}

	row := db.QueryRow("SELECT * FROM USER LIMIT 1")
	name := ""
	err = row.Scan(&name)
	if err != nil {
		hlog.Error("row.Scan err:%v", err)
		return err
	}

	hlog.Info("row.Scan SELECT name:%s", name)

	return nil
}
func main() {
	err := ShowTables()
	if err != nil {
		return
	}

	_ = UserTable()
}

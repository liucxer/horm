package main

import (
	"github.com/liucxer/hlog"
)

type sqliteMaster struct {
	SType    string `json:"type"`
	Name     string `json:"name"`
	TblName  string `json:"tbl_name"`
	RootPage int    `json:"rootpage"`
	Sql      string `json:"sql"`
}

// 查看当前表
func ShowTables(dbPath string) error {
	db, err := NewSqliteDB(dbPath)
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

type User struct {
	Name string `json:"name" orm:"name"`
	Age  int    `json:"age" orm:"age"`
}

func UserTable(dbPath string) error {
	db, err := NewSqliteDB(dbPath)
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
	_, err = db.Exec("CREATE TABLE USER (NAME VARCHAR(255), AGE INT)")
	if err != nil {
		return err
	}

	// 往User表写一行数据
	_, err = db.Exec("INSERT INTO USER (NAME, AGE) VALUES (?,?), (?,?)", "liucx", "30", "wangli", "18")
	if err != nil {
		return err
	}

	var user User
	err = db.QueryRowInto(&user, "SELECT * FROM USER LIMIT 1")
	if err != nil {
		return err
	}

	hlog.Info("user:%+v", user)
	return nil
}
func main() {
	dbPath := "gee.db"
	err := ShowTables(dbPath)
	if err != nil {
		return
	}

	err = UserTable(dbPath)
	if err != nil {
		return
	}
}

package horm_test

import (
	"testing"

	"github.com/liucxer/hlog"
	"github.com/liucxer/horm"
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
	db, err := horm.NewSqliteDB(dbPath)
	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	sqliteMasters := []sqliteMaster{}
	err = db.QueryInto(&sqliteMasters, "select * from sqlite_master")
	if err != nil {
		return err
	}

	hlog.Info("sqliteMaster:%+v", sqliteMasters)
	return nil
}

type User struct {
	Name   string  `json:"name" orm:"name"`
	Age    int     `json:"age" orm:"age"`
	Height float64 `json:"height" orm:"height"`
}

func (u User) TableName() string {
	return "user"
}

type Account struct {
	Name string `json:"name" orm:"name"`
	Age  int    `json:"age" orm:"age"`
}

func (u Account) TableName() string {
	return "account"
}

func UserTable(dbPath string) error {
	db, err := horm.NewSqliteDB(dbPath)
	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	// 删除User表
	_, err = horm.DropTable(User{}).WithDB(db).Exec()
	if err != nil {
		return err
	}

	// 删除Account表
	_, err = horm.DropTable(Account{}).WithDB(db).Exec()
	if err != nil {
		return err
	}

	// 创建User表
	_, err = db.Exec("CREATE TABLE USER (NAME VARCHAR(255), AGE INT(20), Height float64)")
	if err != nil {
		return err
	}

	// 创建Account表
	_, err = db.Exec("CREATE TABLE Account (NAME VARCHAR(255), AGE INT(20))")
	if err != nil {
		return err
	}

	// 往User表写一行数据
	_, err = db.Exec("INSERT INTO USER (NAME, AGE, HEIGHT) VALUES (?,?,?), (?,?,?)", "liucx", "30", "168.1", "wangli", "18", "168.2")
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

func TestDropTable(t *testing.T) {
	var err error
	dbPath := "gee.db"
	err = ShowTables(dbPath)
	if err != nil {
		return
	}

	err = UserTable(dbPath)
	if err != nil {
		return
	}
}

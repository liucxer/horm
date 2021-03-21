package main

import (
	"database/sql"
	"github.com/liucxer/hlog"
	_ "github.com/mattn/go-sqlite3"
	"reflect"
	"strings"
)

type SqliteDB struct {
	db *sql.DB
}

type SqliteExecResult struct {
	LastInsertID int64
	RowsAffected int64
}

func NewSqliteDB(dbPath string) (*SqliteDB, error) {
	// 数据库打开
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		hlog.Error("sql.Open sqlite3 err:%+v, dbPath:%s", err, dbPath)
		return nil, err
	}
	// Send a ping to make sure the database connection is alive.
	if err = db.Ping(); err != nil {
		hlog.Error("db.Ping sqlite3 err:%+v, dbPath:%s", err, dbPath)
		return nil, err
	}
	hlog.Info("sql.Open sqlite3 err:%+v, db:%+v", err, db)
	return &SqliteDB{db: db}, nil
}

func (db *SqliteDB) Close() error {
	err := db.db.Close()
	if err != nil {
		hlog.Error("db.Close err:%+v", err)
		return err
	}
	hlog.Info("db.Close err:%+v", err)
	return err
}

func (db *SqliteDB) Exec(query string, args ...interface{}) (*SqliteExecResult, error) {
	res := SqliteExecResult{}
	result, err := db.db.Exec(query, args...)
	if err != nil {
		hlog.Error("db.Exec err:%+v, query:%s, args:%+v", err, query, args)
		return nil, err
	}
	res.LastInsertID, _ = result.LastInsertId()
	res.RowsAffected, _ = result.RowsAffected()
	hlog.Info("db.Exec err:%+v, query:%s, args:%+v, res:%+v", err, query, args, res)
	return &res, err
}

func (db *SqliteDB) QueryRow(query string, args ...interface{}) *sql.Row {
	row := db.db.QueryRow(query, args...)
	hlog.Info("db.QueryRow query:%s, args:%+v, row:%+v", query, args, row)
	return row
}

type Field struct {
	Name  string
	Kind  reflect.Kind
	Value interface{}
}

// 把rows中的数据转换成Field数组
func RowsToFields(rows *sql.Rows) (*[]Field, error) {
	// 获取每列类型
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		hlog.Error("rows.ColumnTypes err:%v, rows:%+v", err, rows)
		return nil, err
	}

	res := []Field{}
	if rows.Next() {
		items := []interface{}{}
		for _, columnType := range columnTypes {
			columnTypeName := columnType.DatabaseTypeName()
			var field Field
			field.Name = columnType.Name()
			switch {
			case columnTypeName == "INT", columnTypeName == "BIGINT":
				field.Kind = reflect.Int64
				value := int64(0)
				field.Value = &value
				items = append(items, &value)
			case strings.Contains(columnTypeName, "VARCHAR"),
				strings.Contains(columnTypeName, "TEXT"),
				strings.Contains(columnTypeName, "NVARCHAR"):
				value := ""
				field.Value = &value
				field.Kind = reflect.String
				items = append(items, &value)
			case strings.Contains(columnTypeName, "DECIMAL"):
				value := float64(0)
				field.Value = &value
				field.Kind = reflect.Float64
				items = append(items, &value)
			case strings.Contains(columnTypeName, "BOOL"):
				value := false
				field.Value = &value
				field.Kind = reflect.Bool
				items = append(items, &value)
			}
			res = append(res, field)
		}

		err = rows.Scan(items...)
		if err != nil {
			hlog.Error("rows.Scan err:%v, rows:%+v", err, rows)
			return nil, err
		}
	}
	return &res, nil
}

// 把Field对象复制给object
func FieldsToObject(fields *[]Field, object interface{}) error {
	fieldMap := map[string]Field{}
	for _, field := range *fields {
		fieldMap[strings.ToUpper(field.Name)] = field
	}
	objectType := reflect.TypeOf(object)
	objectValue := reflect.ValueOf(object).Elem()
	if objectType.Kind() == reflect.Ptr {
		objectType = objectType.Elem()
	}

	for i := 0; i < objectType.NumField(); i++ {
		f := objectType.Field(i)
		if field, ok := fieldMap[strings.ToUpper(f.Name)]; ok {
			objectField := objectValue.FieldByName(f.Name)
			if objectField.CanSet() {
				switch field.Kind {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					objectField.SetInt(*(field.Value.(*int64)))
				case reflect.String:
					objectField.SetString(*(field.Value.(*string)))
				}
			}
		}
	}
	return nil
}

func (db *SqliteDB) QueryRowInto(object interface{}, query string, args ...interface{}) error {
	// 结果查询
	rows, err := db.db.Query(query, args...)
	if err != nil {
		hlog.Error("db.Query err:%v, query:%s, args:%+v, row:%+v, object:%+v", err, query, args, rows, object)
		return err
	}
	defer func() { _ = rows.Close() }()

	fields, err := RowsToFields(rows)
	if err != nil {
		return err
	}
	err = FieldsToObject(fields, object)
	if err != nil {
		return err
	}
	return nil
}

func (db *SqliteDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.db.Query(query, args...)
	if err != nil {
		hlog.Error("db.Query err:%+v, query:%s, args:%+v", err, query, args)
		return nil, err
	}

	hlog.Info("db.QueryRow query:%s, args:%+v, rows:%+v", query, args, rows)
	return rows, nil
}

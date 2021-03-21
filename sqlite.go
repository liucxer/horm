package main

import (
	"database/sql"
	"errors"
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
	var items []interface{}
	for _, columnType := range columnTypes {
		columnTypeName := strings.ToUpper(columnType.DatabaseTypeName())
		var field Field
		field.Name = columnType.Name()
		switch {
		case strings.HasPrefix(columnTypeName, "INT"),
			strings.HasPrefix(columnTypeName, "BIGINT"):
			field.Kind = reflect.Int64
			value := int64(0)
			field.Value = &value
			items = append(items, &value)
		case strings.HasPrefix(columnTypeName, "VARCHAR"),
			strings.HasPrefix(columnTypeName, "TEXT"),
			strings.HasPrefix(columnTypeName, "NVARCHAR"):
			value := ""
			field.Value = &value
			field.Kind = reflect.String
			items = append(items, &value)
		case strings.HasPrefix(columnTypeName, "DECIMAL"):
			value := float64(0)
			field.Value = &value
			field.Kind = reflect.Float64
			items = append(items, &value)
		case strings.HasPrefix(columnTypeName, "BOOL"):
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
				case reflect.Int64:
					objectField.SetInt(*(field.Value.(*int64)))
				case reflect.String:
					objectField.SetString(*(field.Value.(*string)))
				case reflect.Bool:
					objectField.SetBool(*(field.Value.(*bool)))
				case reflect.Float64:
					objectField.SetFloat(*(field.Value.(*float64)))
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

	if !rows.Next() {
		hlog.Error("rows.Next is false rows:%+v", rows)
		return errors.New("rows is empty")
	}

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

func (db *SqliteDB) QueryInto(object interface{}, query string, args ...interface{}) error {
	objectRV := reflect.ValueOf(object)
	if objectRV.Kind() != reflect.Ptr || objectRV.IsNil() {
		return errors.New("object must be ptr and not nil")
	}

	objectRV = objectRV.Elem()
	// 结果查询
	rows, err := db.db.Query(query, args...)
	if err != nil {
		hlog.Error("db.Query err:%v, query:%s, args:%+v, row:%+v, object:%+v", err, query, args, rows, object)
		return err
	}
	defer func() { _ = rows.Close() }()

	i := 0
	for rows.Next() {
		fields, err := RowsToFields(rows)
		if err != nil {
			return err
		}

		if i >= objectRV.Cap() {
			newcap := objectRV.Cap() + objectRV.Cap()/2
			if newcap < 4 {
				newcap = 4
			}
			newv := reflect.MakeSlice(objectRV.Type(), objectRV.Len(), newcap)
			reflect.Copy(newv, objectRV)
			objectRV.Set(newv)
		}
		if i >= objectRV.Len() {
			objectRV.SetLen(i + 1)
		}

		err = FieldsToObject(fields, objectRV.Index(i))
		if err != nil {
			return err
		}
		i++
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

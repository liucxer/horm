package horm

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/liucxer/hlog"
	_ "github.com/mattn/go-sqlite3"
)

type DB interface {
	Exec(query string, args ...interface{}) (*SqliteExecResult, error)
	QueryRowInto(object interface{}, query string, args ...interface{}) error
	QueryInto(object interface{}, query string, args ...interface{}) error
	Close() error
}

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

	// 数据库连通性测试
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
	// sql语句执行
	res := SqliteExecResult{}
	result, err := db.db.Exec(query, args...)
	if err != nil {
		hlog.Error("db.Exec err:%+v, query:%s, args:%+v", err, query, args)
		return nil, err
	}

	// 获取最后一行插入id和影响行数
	res.LastInsertID, _ = result.LastInsertId()
	res.RowsAffected, _ = result.RowsAffected()
	hlog.Info("db.Exec err:%+v, query:%s, args:%+v, res:%+v", err, query, args, res)
	return &res, err
}

type Field struct {
	TagName string
	Name    string
	Kind    reflect.Kind
	Value   interface{}
}

type FieldList []Field

func (fieldList *FieldList) Marshal() ([]byte, error) {
	tmpMap := map[string]interface{}{}

	for _, field := range *fieldList {
		switch field.Kind {
		case reflect.Int64:
			tmpMap[field.Name] = *(field.Value.(*int64))
		case reflect.String:
			tmpMap[field.Name] = *(field.Value.(*string))
		case reflect.Float64:
			tmpMap[field.Name] = *(field.Value.(*float64))
		case reflect.Bool:
			tmpMap[field.Name] = *(field.Value.(*bool))
		default:
			return nil, errors.New("unknown kind")
		}
	}
	return json.Marshal(tmpMap)
}

type FieldLists []FieldList

func (fieldLists *FieldLists) Marshal() ([]byte, error) {
	tmpMap := []map[string]interface{}{}

	for _, fieldList := range *fieldLists {
		itemMap := map[string]interface{}{}
		for _, field := range fieldList {
			switch field.Kind {
			case reflect.Int64:
				itemMap[field.Name] = *(field.Value.(*int64))
			case reflect.String:
				itemMap[field.Name] = *(field.Value.(*string))
			case reflect.Float64:
				itemMap[field.Name] = *(field.Value.(*float64))
			case reflect.Bool:
				itemMap[field.Name] = *(field.Value.(*bool))
			default:
				hlog.Error("unknown kind field.Kind:%d", field.Kind)
				return nil, errors.New(fmt.Sprintf("unknown kind field.Kind:%d", field.Kind))
			}
		}
		tmpMap = append(tmpMap, itemMap)
	}

	return json.Marshal(tmpMap)
}

func DBModelToFields(dbModel DBModel) (*[]Field, error) {
	rt := reflect.TypeOf(dbModel)
	if rt.Kind() != reflect.Ptr {
		return nil, errors.New("dbModel must be ptr to struct")
	}

	rv := reflect.ValueOf(dbModel)

	elem := rv.Elem()
	if elem.Kind() != reflect.Struct {
		return nil, errors.New("dbModel must be ptr to struct")
	}

	typeOfType := elem.Type()
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		fmt.Printf("%d. %s %s = %v ormTag:%+v \n",
			i, typeOfType.Field(i).Name, field.Type(), field.Interface(), typeOfType.Field(i).Tag.Get("orm"))
	}

	return nil, nil
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
		field.Name = strings.ToLower(columnType.Name())
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
		case strings.HasPrefix(columnTypeName, "DECIMAL"),
			strings.HasPrefix(columnTypeName, "FLOAT"):
			value := float64(0)
			field.Value = &value
			field.Kind = reflect.Float64
			items = append(items, &value)
		case strings.HasPrefix(columnTypeName, "BOOL"):
			value := false
			field.Value = &value
			field.Kind = reflect.Bool
			items = append(items, &value)
		default:
			hlog.Error("unknown columnTypeName columnTypeName:%s", columnTypeName)
			return nil, errors.New(fmt.Sprintf("unknown columnTypeName columnTypeName:%s", columnTypeName))
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

	fieldList := FieldList(*fields)
	bts, err := fieldList.Marshal()
	if err != nil {
		return err
	}

	err = json.Unmarshal(bts, object)
	if err != nil {
		hlog.Error("json.Unmarshal err:%v", err)
		return err
	}

	if rows.Next() {
		hlog.Error("rows.Next is true rows:%+v", rows)
		return errors.New("rows is not only one line")
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

	fieldLists := FieldLists{}
	for rows.Next() {
		fields, err := RowsToFields(rows)
		if err != nil {
			return err
		}

		fieldList := FieldList(*fields)
		fieldLists = append(fieldLists, fieldList)
	}

	bts, err := fieldLists.Marshal()
	if err != nil {
		return err
	}

	err = json.Unmarshal(bts, object)
	if err != nil {
		return err
	}
	return nil
}

type DBModel interface {
	TableName() string
}

func (db *SqliteDB) DropTable(model DBModel) error {
	_, err := db.Exec("DROP TABLE " + strings.ToUpper(model.TableName()))
	return err
}

func (db *SqliteDB) CreateTable(model DBModel) error {
	_, err := db.Exec("CREATE TABLE " + strings.ToUpper(model.TableName()) + " (NAME VARCHAR(255), AGE INT(20), Height float64")
	return err
}

func (db *SqliteDB) List(object interface{}, query string, args ...interface{}) error {
	return db.QueryInto(object, query, args...)
}

package persistent

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func New(address, username, password string) (*DB, error) {
	db := getDBInstance(address, username, password)
	return &DB{Instance: db}, nil
}

type DB struct {
	Instance *sql.DB
}

func (db *DB) Close() error {
	if err := db.Instance.Close(); err != nil {
		log.Fatal("close db error: ", err)
		return err
	}
	return nil
}

func getDBInstance(address, username, password string) *sql.DB {
	config := fmt.Sprintf("%s:%s@tcp(%s)/test?charset=utf8&parseTime=%t&loc=%s",
		username,
		password,
		address,
		true,
		//"Asia/Shanghai"),
		"Local")
	db, err := sql.Open("mysql", config)
	if err != nil {
		panic(err.Error())
	}
	return db
}

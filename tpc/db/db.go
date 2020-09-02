package db

import (
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

type Database interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Close() error
}

func New(address, username, password string) (Database, error) {
	db := getDBInstance(address, username, password)
	return &DB{Instance: db}, nil
}

type DB struct {
	Instance *gorm.DB
}

func (db *DB) Put(key string, value []byte) error {
	db.Instance.AutoMigrate(&KV{})
	kv := &KV{
		Model: gorm.Model{},
		Key:   key,
		Value: string(value),
	}
	db.Instance.Create(&kv)
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	var kv KV
	db.Instance.Where("key=", key).Last(&kv)
	return []byte(kv.Value), nil
}

func (db *DB) Close() error {
	if err := db.Instance.Close(); err != nil {
		log.Fatal("close db error: ", err)
		return err
	}
	return nil
}

func getDBInstance(address, username, password string) *gorm.DB {
	config := fmt.Sprintf("%s:%s@tcp(%s)/test?charset=utf8&parseTime=%t&loc=%s",
		username,
		password,
		address,
		true,
		//"Asia/Shanghai"),
		"Local")
	db, err := gorm.Open("mysql", config)
	if err != nil {
		panic(err.Error())
	}
	return db
}

type KV struct {
	gorm.Model
	Key   string
	Value string
}

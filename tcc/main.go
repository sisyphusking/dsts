package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/sysphusking/dsts/tcc/persistent"

	"github.com/sysphusking/dsts/tcc/example"
	"github.com/sysphusking/dsts/tcc/service"
)

//todo 把这个视频看完：https://www.bilibili.com/video/av837706176/

func main() {
	db, err := persistent.New("localhost", "root", "mysql1012!")
	if err != nil {
		panic(err)
	}
	defer db.Instance.Close()

	accountHandler := example.NewHandlerImpl(db.Instance)
	//初始化一些测试数据，第一次初始化后注释掉
	//if err := setupAccounts(accountHandler); err != nil {
	//	panic(err.Error())
	//}
	transactionStore := service.NewTransactionStore(db.Instance)

	srv := service.NewService(transactionStore, accountHandler)

	ctx := context.Background()

	req := getTransactionRequest("account1", "account2", 10)
	if _, err := srv.StartTransaction(ctx, req); err != nil {
		panic(err)
	}

	t := time.Now().Add(-10000 * time.Millisecond)
	if err := srv.RecoverTransactions(ctx, t); err != nil {
		panic(err)
	}
}

func setupAccounts(ah service.AccountHandler) error {
	accounts := []*example.AccountDoc{
		getAccountDoc("account1", 100),
		getAccountDoc("account2", 100),
	}

	for _, account := range accounts {
		if err := ah.Put(context.Background(), account); err != nil {
			return err
		}
	}
	return nil
}

func getAccountDoc(accountID string, money int) *example.AccountDoc {

	return &example.AccountDoc{
		ID:      accountID,
		Money:   money,
		Version: 0,
	}
}

func getTransactionRequest(source, destination string, itemQuantity int) service.Request {
	return service.Request{
		Source:      source,
		Destination: destination,
		Data:        itemQuantity,
	}
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

package example

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/sysphusking/dsts/tcc/service"
)

type TransactionMethod int

const (
	Increment TransactionMethod = iota
	Decrement
)

var (
	errPendingTransactionIDNotFound = errors.New("pending transaction id not found")
)

const maxUpdateRetry = 4
const UpdateRetryInterval = 100

//version字段： 读取数据时，将version字段的值一同读出，数据每更新一次，对此version值加一。
//			   当我们提交更新的时候，判断数据库表对应记录的当前版本信息与第一次取出来的version值进行比对，
//			   如果数据库表当前版本号与第一次取出来的version值相等，则予以更新，否则认为是过期数据。也可以用时间戳来做

//select... for update是悲观锁，乐观锁可以用上面的方式实现
//每次去拿数据的时候都认为别人会修改，所以每次在拿数据的时候都会上锁，这样别人想拿这个数据就会block直到它拿到锁。

//todo 看看分布式锁，基于Redis和基于Zookeeper的实现方式

//type Resources map[string]Item
//type PendingTransactions []string

type AccountDoc struct {
	ID                  string `json:"id"`
	Money               int    `json:"money"`
	PendingTransactions string `json:"pending_transactions"`
	Version             int    `json:"version"` //version为了实现乐观锁
}

func (a AccountDoc) TableName() string {
	return "account_doc"
}

////使gorm支持[]string结构
//func (c PendingTransactions) Value() (driver.Value, error) {
//	b, err := json.Marshal(c)
//	return string(b), err
//}
//
//func (c PendingTransactions) Scan(input interface{}) error {
//	return json.Unmarshal(input.([]byte), &c)
//}
//
////使gorm支持map[string]Item结构
//func (c Resources) Value() (driver.Value, error) {
//	b, err := json.Marshal(c)
//	return string(b), err
//}
//
//func (c Resources) Scan(input interface{}) error {
//	return json.Unmarshal(input.([]byte), &c)
//}

func (a *AccountDoc) GetID() string {
	return a.ID
}

func (a *AccountDoc) GetPendingTransactions() []string {
	var b []string
	json.Unmarshal([]byte(a.PendingTransactions), &b)
	return b
}

func (a *AccountDoc) GetVersion() int {
	return a.Version
}

type HandlerImpl struct {
	db *sql.DB
}

func NewHandlerImpl(db *sql.DB) *HandlerImpl {
	return &HandlerImpl{db: db}
}

func (h *HandlerImpl) Get(ctx context.Context, accountId string) (service.Account, error) {
	var rs = AccountDoc{}
	if err := h.db.QueryRow("select * from account_doc where id =?", accountId).
		Scan(&rs.ID, &rs.Money, &rs.PendingTransactions, &rs.Version); err != nil {
		return nil, err
	}
	return &rs, nil
}

func (h *HandlerImpl) Put(ctx context.Context, doc service.Account) error {

	accountDoc, ok := doc.(*AccountDoc)
	if !ok {
		return errors.New("accountDoc  not implements Account")
	}
	stm, err := h.db.Prepare("insert into account_doc(id, money,pending_transactions,version) values (?,?,?,?)")
	if err != nil {
		return err
	}
	if _, err := stm.Exec(accountDoc.ID, accountDoc.Money, accountDoc.PendingTransactions, accountDoc.Version); err != nil {
		return err
	}
	return nil
}

func (h *HandlerImpl) Update(ctx context.Context, accountId, transactionId string, tr service.Request) error {
	req, ok := tr.Data.(int)
	if !ok {
		return fmt.Errorf("type error")
	}
	method := Decrement
	if accountId == tr.Destination {
		method = Increment
	}
	//重试机制
	//实现幂等操作
	for i := 0; i < maxUpdateRetry; i++ {
		err := h.findAndModify(ctx, accountId, transactionId, req, method)
		if err == nil {
			return nil
		}
		time.Sleep(UpdateRetryInterval * time.Millisecond)
	}
	return errors.New("update failed because the process has reached max retry times")
}

//通过version字段实现乐观锁，这里的实现逻辑和rollback正好相反
func (h *HandlerImpl) findAndModify(ctx context.Context, accountId, transactionId string, tr int, method TransactionMethod) error {

	account, err := h.Get(ctx, accountId)
	if err != nil {
		return err
	}
	accountDoc, ok := account.(*AccountDoc)
	if !ok {
		return err
	}
	//取出原先保存的version
	currentVersion := accountDoc.Version

	money := accountDoc.Money
	if method == Decrement {
		money -= tr
	} else if method == Increment {
		money += tr
	} else {
		return errors.New("error method")
	}

	log.Println("update before :", accountId, accountDoc)

	//transaction加入到队列中
	pts := parseString(accountDoc.PendingTransactions)
	pts = append(pts, transactionId)

	ptsString := convertString(pts)
	version := currentVersion + 1

	log.Println("update after :", accountId, money, ptsString, currentVersion, version)
	stm, err := h.db.Exec("update account_doc set money =? , pending_transactions = ? , version=? where id =? and version=?",
		money, ptsString, version, accountId, currentVersion)
	if err != nil {
		return err
	}
	row, _ := stm.RowsAffected()
	//如果更新成功的行数为0，说明在此事务执行期间，有事务更新了这条记录。需要丢弃，进行重试
	if row == 0 {
		return errors.New("data has updated by other transactions ")
	}
	return nil
}

func (h *HandlerImpl) Commit(ctx context.Context, accountId, transactionId string) error {

	for i := 0; i < maxUpdateRetry; i++ {
		err := h.commit(ctx, accountId, transactionId)
		if err == nil {
			return nil
		}
		time.Sleep(UpdateRetryInterval * time.Millisecond)
	}
	return errors.New("commit failed because the process has reached max retry times")
}

func (h *HandlerImpl) commit(ctx context.Context, accountId, transactionId string) error {
	account, err := h.Get(ctx, accountId)
	if err != nil {
		return err
	}
	accountDoc, ok := account.(*AccountDoc)
	if !ok {
		return err
	}
	log.Println("commit before:", accountDoc)

	//取出原先保存的version
	currentVersion := accountDoc.Version

	//获取pending事务列表中某个transaction的索引
	ptIndex, err := getPendingTransactionIndex(accountDoc.GetPendingTransactions(), transactionId)
	//如果列表中没有找到当前的transactionid，直接返回
	if h.IsErrorPendingTransactionIdNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	//移除transactionId
	pts := parseString(accountDoc.PendingTransactions)
	pts = append(pts[:ptIndex], pts[ptIndex+1:]...)
	accountDoc.PendingTransactions = convertString(pts)
	//版本号加1
	accountDoc.Version += 1

	log.Println("commit after:", accountDoc)
	//保存更新后的数据
	stm, err := h.db.Exec("update account_doc set  pending_transactions = ? , version=? where id =? and version =? ",
		accountDoc.PendingTransactions, accountDoc.Version, accountId, currentVersion)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	row, _ := stm.RowsAffected()
	//如果更新成功的行数为0，说明在此事务执行期间，有事务更新了这条记录。需要丢弃，进行重试
	if row == 0 {
		return errors.New("data has updated by other transactions ")
	}
	return nil
}

func (h *HandlerImpl) Rollback(ctx context.Context, accountId, transactionId string, tr service.Request) error {
	req, ok := tr.Data.(int)
	if !ok {
		return fmt.Errorf("type error")
	}
	method := Increment
	if accountId == tr.Destination {
		method = Decrement
	}
	for i := 0; i < maxUpdateRetry; i++ {
		err := h.rollback(ctx, accountId, transactionId, req, method)
		if err == nil {
			return nil
		}
		//todo 这里可以添加其他error,比如网络不通等，否则一直重试

		time.Sleep(UpdateRetryInterval * time.Millisecond)
	}
	return errors.New("rollback failed because the process has reached max retry times")
}

func (h *HandlerImpl) rollback(ctx context.Context, accountId, transactionId string, tr int, method TransactionMethod) error {
	account, err := h.Get(ctx, accountId)
	if err != nil {
		return err
	}
	accountDoc, ok := account.(*AccountDoc)
	if !ok {
		return err
	}
	log.Println("rollback before:", accountDoc)
	//取出原先保存的version
	currentVersion := accountDoc.Version
	//取出原先的item
	money := accountDoc.Money

	if method == Decrement {
		money -= tr
	} else if method == Increment {
		money += tr
	} else {
		return errors.New("error method")
	}

	//获取pending事务列表中transactionId的索引
	ptIndex, err := getPendingTransactionIndex(accountDoc.GetPendingTransactions(), transactionId)
	//如果列表中没有找到当前的transactionId，直接返回
	if h.IsErrorPendingTransactionIdNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	//移除transactionId
	pts := parseString(accountDoc.PendingTransactions)
	pts = append(pts[:ptIndex], pts[ptIndex+1:]...)
	ptsString := convertString(pts)
	version := currentVersion + 1

	log.Println("rollback after:", accountDoc)
	//保存更新后的数据
	stm, err := h.db.Exec("update account_doc set money = ?  pending_transactions = ? , version=? where id =? and version =? ",
		money, ptsString, version, accountId, currentVersion)
	if err != nil {
		return err
	}
	row, _ := stm.RowsAffected()
	//如果更新成功的行数为0，说明在此事务执行期间，有事务更新了这条记录。需要丢弃，进行重试
	if row == 0 {
		return errors.New("data has updated by other transactions ")
	}
	return nil
}

func (h *HandlerImpl) IsErrorPendingTransactionIdNotFound(err error) bool {
	return err == errPendingTransactionIDNotFound
}

func getPendingTransactionIndex(pts []string, st string) (int, error) {
	for i, pt := range pts {
		if pt == st {
			return i, nil
		}
	}
	return 0, errPendingTransactionIDNotFound
}

func parseString(s string) []string {
	var d []string
	json.Unmarshal([]byte(s), &d)
	return d
}

func convertString(s []string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

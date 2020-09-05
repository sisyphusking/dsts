package service

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
)

const (
	Pending TransactionState = iota
	Applied
	Done
	Canceling
	Cancelled
)

type TransactionState int

type TransactionHandler interface {
	Insert(ctx context.Context, source, destination, reference string, data interface{}) (string, error)
	UpdateState(ctx context.Context, id string, newStata TransactionState) (*Transaction, error)
	GetTransaction(ctx context.Context, id string) (*Transaction, error)
	GetTransactionsInState(ctx context.Context, state TransactionState, query string) ([]*Transaction, error)
	GetAllTransactionsInState(ctx context.Context, state TransactionState) ([]*Transaction, error)
}

type Transaction struct {
	ID                   string           `json:"id"`
	TransactionReference string           `json:"transaction_reference"`
	TransactionState     TransactionState `json:"transaction_state"`
	Source               string           `json:"source"`
	Destination          string           `json:"destination"`
	Value                int              `json:"value"`
	LastModified         time.Time        `json:"last_modified"`
}

func (t Transaction) TableName() string {
	return "transaction"
}

type TransactionStore struct {
	db *sql.DB
}

func (t *TransactionStore) Insert(ctx context.Context, source, destination, reference string, data interface{}) (string, error) {

	id := uuid.New().String()
	ts := Transaction{
		ID:                   id,
		TransactionReference: reference,
		TransactionState:     Pending,
		Source:               source,
		Destination:          destination,
		Value:                data.(int), //todo 这里的interface不好转化成数据库里的对象
		LastModified:         time.Now(),
	}

	stm, err := t.db.Prepare("insert into transaction(id, transaction_reference,transaction_state," +
		"source,destination, value, last_modified ) values (?,?,?,?,?,?,?)")

	if err != nil {
		return "", err
	}
	if _, err := stm.Exec(ts.ID, ts.TransactionReference, ts.TransactionState, ts.Source, ts.Destination,
		ts.Value, ts.LastModified); err != nil {
		return "", err
	}

	return id, nil
}

func (t *TransactionStore) UpdateState(ctx context.Context, id string, newStata TransactionState) (*Transaction, error) {
	ts := &Transaction{}
	_, err := t.db.Exec("update transaction set  transaction_state = ? , last_modified=? where id =?  ",
		newStata, time.Now(), id)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	return ts, nil
}

func (t *TransactionStore) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	ts := &Transaction{}
	if err := t.db.QueryRow("select * from transaction where id?", id).Scan(&ts.ID, &ts.TransactionReference,
		&ts.TransactionState, &ts.Source, &ts.Destination, &ts.LastModified, &ts.Value); err != nil {
		return nil, err
	}

	return ts, nil
}

func (t *TransactionStore) GetTransactionsInState(ctx context.Context, state TransactionState, query string) ([]*Transaction, error) {

	tsList := make([]*Transaction, 0)
	results, err := t.db.Query("select * from transaction where transaction_state=? "+
		"and transaction_reference =?", state, query)
	if err != nil {
		return nil, err
	}

	for results.Next() {
		var tr Transaction
		if err := results.Scan(&tr.ID, &tr.TransactionReference, &tr.TransactionState, &tr.Source, &tr.Destination, &tr.LastModified, &tr.Value); err != nil {
			return nil, err
		}
		tsList = append(tsList, &tr)
	}

	return tsList, nil
}

func (t *TransactionStore) GetAllTransactionsInState(ctx context.Context, state TransactionState) ([]*Transaction, error) {

	tsList := make([]*Transaction, 0)
	results, err := t.db.Query("select * from transaction where transaction_state=? ", state)
	if err != nil {
		return nil, err
	}
	for results.Next() {
		var tr Transaction
		if err := results.Scan(&tr.ID, &tr.TransactionReference, &tr.TransactionState, &tr.Source,
			&tr.Destination, &tr.LastModified, &tr.Value); err != nil {
			return nil, err
		}
		tsList = append(tsList, &tr)
	}

	return tsList, nil
}

func NewTransactionStore(db *sql.DB) *TransactionStore {
	return &TransactionStore{db: db}
}

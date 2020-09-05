package service

import (
	"context"
	"time"
)

type Account interface {
	GetID() string
	GetPendingTransactions() []string
	GetVersion() int
}

type AccountHandler interface {
	Get(ctx context.Context, accountId string) (Account, error)
	Put(ctx context.Context, doc Account) error
	Update(ctx context.Context, accountId, transactionId string, tr Request) error
	Rollback(ctx context.Context, accountId, transactionId string, tr Request) error
	Commit(ctx context.Context, accountId, transactionId string) error
	IsErrorPendingTransactionIdNotFound(err error) bool
}

type Request struct {
	Source      string
	Destination string
	Reference   string
	Data        interface{}
}

type Response struct {
	TransactionID string
	LastModified  int64
}

type Service struct {
	Ts TransactionHandler
	Ah AccountHandler
}

func NewService(ts TransactionHandler, ah AccountHandler) *Service {
	return &Service{Ts: ts, Ah: ah}
}

func (s *Service) applyTransaction(ctx context.Context, req Request, transactionId string, callbacks ...func() error) error {
	//更新原始账户
	if err := s.Ah.Update(ctx, req.Source, transactionId, req); err != nil {
		return err
	}
	//更新目标账户
	if err := s.Ah.Update(ctx, req.Destination, transactionId, req); err != nil {
		return err
	}

	if len(callbacks) > 0 {
		for _, f := range callbacks {
			if err := f(); err != nil {
				return err
			}
		}
	}

	//上面的两个都更新成功了，该表事务事务状态为applied
	if _, err := s.Ts.UpdateState(ctx, transactionId, Applied); err != nil {
		//更新事务状态失败，返回error
		return err
	}
	return nil
}

func (s *Service) commitTransaction(ctx context.Context, req Request, transactionId string) (*Transaction, error) {
	//提交source
	if err := s.Ah.Commit(ctx, req.Source, transactionId); err != nil {
		return nil, err
	}
	//提交destination
	if err := s.Ah.Commit(ctx, req.Destination, transactionId); err != nil {
		return nil, err
	}

	//上面都成功了，就将事务表的状态改成done
	tr, err := s.Ts.UpdateState(ctx, transactionId, Done)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func (s *Service) cancelTransaction(ctx context.Context, req Request, transactionId string) error {
	//回滚目标事务
	if err := s.Ah.Rollback(ctx, req.Destination, transactionId, req); err != nil {
		//如果不是transactionId没找到的错误，都立即返回error
		if !s.Ah.IsErrorPendingTransactionIdNotFound(err) {
			return err
		}
	}
	//回滚原事务
	if err := s.Ah.Rollback(ctx, req.Source, transactionId, req); err != nil {
		if !s.Ah.IsErrorPendingTransactionIdNotFound(err) {
			return err
		}
	}
	//上面都成功了，将事务表中的状态修改为cancelled
	if _, err := s.Ts.UpdateState(ctx, transactionId, Cancelled); err != nil {
		return err
	}
	return nil
}

func (s *Service) recoverTransaction(ctx context.Context, ts []*Transaction,
	recoverTime time.Time, state TransactionState) error {
	if len(ts) > 0 {
		for _, t := range ts {
			if recoverTime.After(t.LastModified) {
				req := Request{
					Source:      t.Source,
					Destination: t.Destination,
					Data:        t.Value,
				}
				if err := s.recoverFromError(ctx, t.ID, req, state); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Service) recoverFromError(ctx context.Context, transactionId string, req Request, state TransactionState) error {
	switch state {
	case Pending:
		//把事务表中的状态从pending更新为canceling，事务还没有apply
		if _, err := s.Ts.UpdateState(ctx, transactionId, Canceling); err != nil {
			return err
		}
		return s.cancelTransaction(ctx, req, transactionId)
	case Applied:
		//两个事务都是已经执行成功了，但是还没有到done的阶段，重新提交
		if _, err := s.commitTransaction(ctx, req, transactionId); err != nil {
			return err
		}
		return nil
	case Canceling:
		//canceling状态的直接取消事务，canceling状态是pending转变的，pending回滚这个过程如果失败了，这里继续处理
		return s.cancelTransaction(ctx, req, transactionId)
	default:
		return nil
	}
}

func (s *Service) StartTransaction(ctx context.Context, req Request, callbacks ...func() error) (*Response, error) {
	//事务表中插入记录
	transactionId, err := s.Ts.Insert(ctx, req.Source, req.Destination, req.Reference, req.Data)
	if err != nil {
		//失败了直接返回，不用回滚
		return nil, err
	}

	//apply阶段
	if err := s.applyTransaction(ctx, req, transactionId, callbacks...); err != nil {
		//pending状态的回滚
		if err := s.recoverFromError(ctx, transactionId, req, Pending); err != nil {
			return nil, err
		}
		return nil, err
	}
	//commit阶段
	tr, err := s.commitTransaction(ctx, req, transactionId)
	if err != nil {
		if err := s.recoverFromError(ctx, transactionId, req, Applied); err != nil {
			return nil, err
		}
		return nil, err
	}
	return &Response{
		TransactionID: transactionId,
		LastModified:  tr.LastModified.Unix(),
	}, nil
}

func (s *Service) GetTransactions(ctx context.Context, state TransactionState, query string) ([]*Transaction, error) {
	return s.Ts.GetTransactionsInState(ctx, state, query)
}

//recoverTime是确保新加入的事务不会别加入到回滚列表
func (s *Service) RecoverTransactions(ctx context.Context, recoverTime time.Time) error {
	//遍历这几种状态,cancelled的除外
	lt := []TransactionState{Canceling, Applied, Pending}
	for k, state := range lt {
		transactionList, err := s.Ts.GetAllTransactionsInState(ctx, lt[k])
		if err != nil {
			return err
		}
		if err := s.recoverTransaction(ctx, transactionList, recoverTime, state); err != nil {
			return err
		}
	}
	return nil
}

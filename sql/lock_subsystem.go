package sql

import (
	"gopkg.in/src-d/go-errors.v1"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var ErrLockTimeout = errors.NewKind("Timeout acquiring lock '%s'.")
var ErrLockDoesNotExist = errors.NewKind("Lock '%s' does not exist.")
var ErrLockNotOwned = errors.NewKind("Operation '%s' failed as the lock '%s' has a different owner.")

type ownedLock struct {
	Owner int64
	Count int64
}

type LockSubsystem struct {
	lockLock *sync.RWMutex
	locks map[string]**ownedLock
}

func NewLockSubsystem() *LockSubsystem {
	return &LockSubsystem{&sync.RWMutex{}, make(map[string]**ownedLock)}
}

func (ls *LockSubsystem) getNamedLock(name string) **ownedLock {
	ls.lockLock.RLock()
	defer ls.lockLock.RUnlock()

	return ls.locks[name]
}

func (ls *LockSubsystem) createLock(name string) **ownedLock {
	ls.lockLock.Lock()
	defer ls.lockLock.Unlock()

	nl, ok := ls.locks[name]

	if !ok {
		newLock := &ownedLock{}
		ls.locks[name] = &newLock
		nl = &newLock
	}

	return  nl
}

func (ls *LockSubsystem) Lock(ctx *Context, name string, timeout time.Duration) error {
	nl := ls.getNamedLock(name)

	if nl == nil {
		nl = ls.createLock(name)
	}

	userId := int64(ctx.Session.ID())
	for i, start := 0, time.Now(); i == 0 || timeout < 0 || time.Since(start) < timeout; i++ {
		dest := (*unsafe.Pointer)(unsafe.Pointer(nl))
		curr := atomic.LoadPointer(dest)
		currLock := *(*ownedLock)(curr)

		if currLock.Owner == 0 {
			newVal := &ownedLock{userId, 1}
			if atomic.CompareAndSwapPointer(dest, curr, unsafe.Pointer(newVal)) {
				return ctx.Session.AddLock(name)
			}
		} else if currLock.Owner == userId {
			newVal := &ownedLock{userId, currLock.Count + 1}
			if atomic.CompareAndSwapPointer(dest, curr, unsafe.Pointer(newVal)) {
				return nil
			}
		}

		time.Sleep(100*time.Microsecond)
	}

	return ErrLockTimeout.New(name)
}

func (ls *LockSubsystem) Unlock(ctx *Context, name string) error {
	nl := ls.getNamedLock(name)

	if nl == nil {
		return ErrLockDoesNotExist.New(name)
	}

	userId := int64(ctx.Session.ID())
	for {
		dest := (*unsafe.Pointer)(unsafe.Pointer(nl))
		curr := atomic.LoadPointer(dest)
		currLock := *(*ownedLock)(curr)

		if currLock.Owner != userId {
			return ErrLockNotOwned.New("unlock", name)
		}

		newVal := &ownedLock{}
		if currLock.Count > 1 {
			newVal = &ownedLock{userId, currLock.Count - 1}
		}

		if atomic.CompareAndSwapPointer(dest, curr, unsafe.Pointer(newVal)) {
			if newVal.Count == 0 {
				return ctx.Session.DelLock(name)
			}

			return nil
		}
	}
}

func (ls *LockSubsystem) ReleaseAll(ctx *Context) (int, error) {
	releaseCount := 0
	_ = ctx.Session.IterLocks(func(name string) error {
		nl := ls.getNamedLock(name)

		if nl != nil {
			userId := ctx.Session.ID()
			for {
				dest := (*unsafe.Pointer)(unsafe.Pointer(nl))
				curr := atomic.LoadPointer(dest)
				currLock := *(*ownedLock)(curr)

				if currLock.Owner != int64(userId) {
					break
				}

				if atomic.CompareAndSwapPointer(dest, curr, unsafe.Pointer(&ownedLock{})) {
					releaseCount++
					break
				}
			}
		}

		return nil
	})

	return releaseCount, nil
}

type LockState int

const (
	LockDoesNotExist LockState = iota
	LockInUse
	LockFree
)

func (ls *LockSubsystem) GetLockState(name string) (state LockState, owner uint32) {
	nl := ls.getNamedLock(name)

	if nl == nil {
		return LockDoesNotExist, 0
	}

	dest := (*unsafe.Pointer)(unsafe.Pointer(nl))
	curr := atomic.LoadPointer(dest)
	currLock := *(*ownedLock)(curr)

	if currLock.Owner == 0 {
		return LockFree, 0
	} else {
		return LockInUse, uint32(currLock.Owner)
	}
}

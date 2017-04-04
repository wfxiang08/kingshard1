package backend

import (
	"sync"
	"sync/atomic"
	"time"

	"core/errors"
	"mysql"
)

const (
	Up = iota
	Down
	ManualDown
	Unknown

	InitConnCount           = 16
	DefaultMaxConnNum       = 1024
	PingPeroid        int64 = 4
)

type DB struct {
	sync.RWMutex

	// 一个DB，对应后台的一个数据库，可能是Master，也可能是Slave
	addr     string
	user     string
	password string
	db       string
	state    int32

	maxConnNum  int
	InitConnNum int

	// 一个DB后面对应着多个Connection
	// cacheConns 复用可用的Connection
	// idleConns 复用Connection对象，避免内存分配
	idleConns  chan *Conn
	cacheConns chan *Conn

	// 专门用于做Ping等处理的
	checkConn *Conn
	lastPing  int64
}

// 创建一个DB
func Open(addr string, user string, password string, dbName string, maxConnNum int) (*DB, error) {
	var err error

	db := new(DB)
	db.addr = addr
	db.user = user
	db.password = password
	db.db = dbName

	// InitConnNum 和 MaxConnNum
	if 0 < maxConnNum {
		db.maxConnNum = maxConnNum
		if db.maxConnNum < 16 {
			db.InitConnNum = db.maxConnNum
		} else {
			db.InitConnNum = db.maxConnNum / 4
		}
	} else {
		db.maxConnNum = DefaultMaxConnNum
		db.InitConnNum = InitConnCount
	}

	// 专门用于检测数据库是否还活着
	//check connection
	db.checkConn, err = db.newConn()
	if err != nil {
		db.Close()
		return nil, err
	}

	// 创建两个 chan
	db.idleConns = make(chan *Conn, db.maxConnNum)
	db.cacheConns = make(chan *Conn, db.maxConnNum)

	// DB的初始状态是不确定的
	atomic.StoreInt32(&(db.state), Unknown)

	// 这里是什么逻辑呢?
	// cacheConns/idleConns
	// idleConns 只有Conn对象，没有实际的连接
	//
	for i := 0; i < db.maxConnNum; i++ {
		if i < db.InitConnNum {
			conn, err := db.newConn()
			if err != nil {
				db.Close()
				return nil, err
			}
			conn.pushTimestamp = time.Now().Unix()
			db.cacheConns <- conn
		} else {
			// idleConns只是为了复用Conn对象，避免频繁的内存分配
			conn := new(Conn)
			db.idleConns <- conn
		}
	}
	db.SetLastPing()

	return db, nil
}

func (db *DB) Addr() string {
	return db.addr
}

func (db *DB) State() string {
	var state string
	switch db.state {
	case Up:
		state = "up"
	case Down, ManualDown:
		state = "down"
	case Unknown:
		state = "unknow"
	}
	return state
}

func (db *DB) IdleConnCount() int {
	db.RLock()
	defer db.RUnlock()
	return len(db.cacheConns)
}

func (db *DB) Close() error {
	db.Lock()
	idleChannel := db.idleConns
	cacheChannel := db.cacheConns
	db.cacheConns = nil
	db.idleConns = nil
	db.Unlock()
	if cacheChannel == nil || idleChannel == nil {
		return nil
	}

	close(cacheChannel)
	for conn := range cacheChannel {
		db.closeConn(conn)
	}
	close(idleChannel)

	return nil
}

func (db *DB) getConns() (chan *Conn, chan *Conn) {
	db.RLock()
	cacheConns := db.cacheConns
	idleConns := db.idleConns
	db.RUnlock()
	return cacheConns, idleConns
}

func (db *DB) getCacheConns() chan *Conn {
	db.RLock()
	conns := db.cacheConns
	db.RUnlock()
	return conns
}

func (db *DB) getIdleConns() chan *Conn {
	db.RLock()
	conns := db.idleConns
	db.RUnlock()
	return conns
}

func (db *DB) Ping() error {
	var err error

	// 专门的checkConn
	if db.checkConn == nil {
		db.checkConn, err = db.newConn()
		if err != nil {
			db.closeConn(db.checkConn)
			db.checkConn = nil
			return err
		}
	}
	err = db.checkConn.Ping()
	if err != nil {
		db.closeConn(db.checkConn)
		db.checkConn = nil
		return err
	}
	return nil
}

//
// 创建一个到后端MySQL
//
func (db *DB) newConn() (*Conn, error) {
	co := new(Conn)
	if err := co.Connect(db.addr, db.user, db.password, db.db); err != nil {
		return nil, err
	}

	return co, nil
}

func (db *DB) closeConn(co *Conn) error {
	if co != nil {
		co.Close()

		// idleConns中保存的是 closed Conn
		conns := db.getIdleConns()
		if conns != nil {
			select {
			case conns <- co:
				return nil
			default:
				return nil
			}
		}
	}
	return nil
}

func (db *DB) tryReuse(co *Conn) error {
	var err error
	//reuse Connection
	if co.IsInTransaction() {
		//we can not reuse a connection in transaction status
		err = co.Rollback()
		if err != nil {
			return err
		}
	}

	if !co.IsAutoCommit() {
		//we can not  reuse a connection not in autocomit
		_, err = co.exec("set autocommit = 1")
		if err != nil {
			return err
		}
	}

	//connection may be set names early
	//we must use default utf8
	// TODO: 优化这部分的设计
	if co.GetCharset() != mysql.DEFAULT_CHARSET {
		err = co.SetCharset(mysql.DEFAULT_CHARSET, mysql.DEFAULT_COLLATION_ID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) PopConn() (*Conn, error) {
	var co *Conn
	var err error

	cacheConns, idleConns := db.getConns()
	if cacheConns == nil || idleConns == nil {
		return nil, errors.ErrDatabaseClose
	}

	// 优先考虑: cache
	co = db.GetConnFromCache(cacheConns)

	// 读取idles
	if co == nil {
		co, err = db.GetConnFromIdle(cacheConns, idleConns)
		if err != nil {
			return nil, err
		}
	}

	// 重用Connection
	err = db.tryReuse(co)
	if err != nil {
		db.closeConn(co)
		return nil, err
	}

	return co, nil
}

//
// 从Cache中获取一个 DB
//
func (db *DB) GetConnFromCache(cacheConns chan *Conn) *Conn {
	var co *Conn
	var err error
	for 0 < len(cacheConns) {
		// 获取一个Cache的Connection
		co = <-cacheConns
		// 需要主动去Ping一下后端服务器
		if co != nil && PingPeroid < time.Now().Unix()-co.pushTimestamp {
			err = co.Ping()

			// Ping失败了，再选择下一个
			if err != nil {
				db.closeConn(co)
				co = nil
			}
		}
		if co != nil {
			break
		}
	}
	return co
}

// 从 cacheConn或 idleConns中获取一个Connection
func (db *DB) GetConnFromIdle(cacheConns, idleConns chan *Conn) (*Conn, error) {
	var co *Conn
	var err error
	select {
	case co = <-idleConns:
		err = co.Connect(db.addr, db.user, db.password, db.db)
		if err != nil {
			db.closeConn(co)
			return nil, err
		}
		return co, nil
	case co = <-cacheConns:
		if co == nil {
			return nil, errors.ErrConnIsNil
		}
		if co != nil && PingPeroid < time.Now().Unix()-co.pushTimestamp {
			err = co.Ping()
			if err != nil {
				db.closeConn(co)
				return nil, errors.ErrBadConn
			}
		}
	}
	return co, nil
}

//
// 用完之后归还Connection, 如果不Cache, 则直接关闭
//
func (db *DB) PushConn(co *Conn, err error) {
	if co == nil {
		return
	}
	conns := db.getCacheConns()
	if conns == nil {
		co.Close()
		return
	}
	if err != nil {
		db.closeConn(co)
		return
	}
	co.pushTimestamp = time.Now().Unix()
	select {
	case conns <- co:
		return
	default:
		db.closeConn(co)
		return
	}
}

// 后端连接：
type BackendConn struct {
	*Conn
	db *DB
}

func (p *BackendConn) Close() {
	if p != nil && p.Conn != nil {
		if p.Conn.pkgErr != nil {
			p.db.closeConn(p.Conn)
		} else {
			p.db.PushConn(p.Conn, nil)
		}
		p.Conn = nil
	}
}

func (db *DB) GetConn() (*BackendConn, error) {
	c, err := db.PopConn()
	if err != nil {
		return nil, err
	}
	return &BackendConn{c, db}, nil
}

func (db *DB) SetLastPing() {
	db.lastPing = time.Now().Unix()
}

func (db *DB) GetLastPing() int64 {
	return db.lastPing
}

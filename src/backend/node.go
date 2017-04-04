// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package backend

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kingshard/config"
	"kingshard/core/errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

const (
	Master      = "master"
	Slave       = "slave"
	SlaveSplit  = ","
	WeightSplit = "@"
)

// Node是什么概念呢?
// 1. Node对应配置文案中的node的定义，分为master, slave
// 2. 内部包含多个DB, 每一个DB有自己的可用性检测
//
type Node struct {
	Cfg config.NodeConfig

	sync.RWMutex
	Master *DB
	Slave  []*DB

	LastSlaveIndex int
	RoundRobinQ    []int
	SlaveWeights   []int

	DownAfterNoAlive time.Duration
}

func (n *Node) CheckNode() {
	//to do
	//1 check connection alive
	for {
		n.checkMaster()
		n.checkSlave()
		time.Sleep(16 * time.Second)
	}
}

func (n *Node) String() string {
	return n.Cfg.Name
}

// 获取Master的Connection
func (n *Node) GetMasterConn() (*BackendConn, error) {
	db := n.Master
	if db == nil {
		return nil, errors.ErrNoMasterConn
	}
	if atomic.LoadInt32(&(db.state)) == Down {
		return nil, errors.ErrMasterDown
	}

	return db.GetConn()
}

// 获取一个Slave? DB的选择很重要
func (n *Node) GetSlaveConn() (*BackendConn, error) {
	n.Lock()

	// 获取下一个Slave
	db, err := n.GetNextSlave()
	n.Unlock()
	if err != nil {
		return nil, err
	}

	// 可能失败，也可能是Down? 如果是Down, 为什么要返回呢?
	if db == nil {
		return nil, errors.ErrNoSlaveDB
	}

	// Down/ManualDown 都是Down吧?
	if atomic.LoadInt32(&(db.state)) == Down {
		return nil, errors.ErrSlaveDown
	}

	return db.GetConn()
}

func (n *Node) checkMaster() {
	db := n.Master
	if db == nil {
		log.Errorf("Node: checkMaster Master is not alive")
		return
	}

	// 定期对数据库进行Ping
	if err := db.Ping(); err != nil {
		// Ping失败了，如何处理呢?
		log.ErrorErrorf(err, "Node: checkMaster ping db.Addr: %s", db.Addr())
	} else {
		// ping成功了，则更新状态
		if atomic.LoadInt32(&(db.state)) == Down {
			log.ErrorErrorf(err, "Node: checkMaster Master up db.Addr: %s", db.Addr())
			n.UpMaster(db.addr)
		}
		db.SetLastPing()
		if atomic.LoadInt32(&(db.state)) != ManualDown {
			atomic.StoreInt32(&(db.state), Up)
		}
		return
	}

	// 标记Master数据库挂了
	if int64(n.DownAfterNoAlive) > 0 && time.Now().Unix()-db.GetLastPing() > int64(n.DownAfterNoAlive/time.Second) {
		log.Printf("Node: checkMaster Master down db.Addr: %s, Master_down_time: %d",
			db.Addr(), int64(n.DownAfterNoAlive/time.Second))
		n.DownMaster(db.addr, Down)
	}
}

func (n *Node) checkSlave() {
	n.RLock()
	if n.Slave == nil {
		n.RUnlock()
		return
	}

	// 拷贝一份
	slaves := make([]*DB, len(n.Slave))
	copy(slaves, n.Slave)
	n.RUnlock()

	for i := 0; i < len(slaves); i++ {
		if err := slaves[i].Ping(); err != nil {
			log.ErrorErrorf(err, "Node checkSlave, Ping: %s", slaves[i].Addr())
		} else {
			// 如果OK, 则启动DB
			if atomic.LoadInt32(&(slaves[i].state)) == Down {
				log.Printf("Node checkSlave, Slave up: %s", slaves[i].Addr())
				n.UpSlave(slaves[i].addr)
			}
			slaves[i].SetLastPing()
			if atomic.LoadInt32(&(slaves[i].state)) != ManualDown {
				atomic.StoreInt32(&(slaves[i].state), Up)
			}
			continue
		}

		if int64(n.DownAfterNoAlive) > 0 && time.Now().Unix()-slaves[i].GetLastPing() > int64(n.DownAfterNoAlive/time.Second) {

			log.Printf("Node checkSlave, Slave down: %s, slave_down_time: %d", slaves[i].Addr(), int64(n.DownAfterNoAlive/time.Second))
			//
			//TODO: 为什么不直接传递slaves[i]呢?
			//
			n.DownSlave(slaves[i].addr, Down)
		}
	}

}

// 添加一个Slave, 添加之后，InitBalancer
func (n *Node) AddSlave(addr string) error {
	var db *DB
	var weight int
	var err error
	if len(addr) == 0 {
		return errors.ErrAddressNull
	}
	n.Lock()
	defer n.Unlock()
	for _, v := range n.Slave {
		if strings.Split(v.addr, WeightSplit)[0] == strings.Split(addr, WeightSplit)[0] {
			return errors.ErrSlaveExist
		}
	}
	addrAndWeight := strings.Split(addr, WeightSplit)
	if len(addrAndWeight) == 2 {
		weight, err = strconv.Atoi(addrAndWeight[1])
		if err != nil {
			return err
		}
	} else {
		weight = 1
	}
	n.SlaveWeights = append(n.SlaveWeights, weight)
	if db, err = n.OpenDB(addrAndWeight[0]); err != nil {
		return err
	} else {
		n.Slave = append(n.Slave, db)
		n.InitBalancer()
		return nil
	}
}

// 删除一个Slave, InitBalancer
func (n *Node) DeleteSlave(addr string) error {
	var i int
	n.Lock()
	defer n.Unlock()
	slaveCount := len(n.Slave)
	if slaveCount == 0 {
		return errors.ErrNoSlaveDB
	}
	for i = 0; i < slaveCount; i++ {
		if n.Slave[i].addr == addr {
			break
		}
	}
	if i == slaveCount {
		return errors.ErrSlaveNotExist
	}
	if slaveCount == 1 {
		n.Slave = nil
		n.SlaveWeights = nil
		n.RoundRobinQ = nil
		return nil
	}

	s := make([]*DB, 0, slaveCount-1)
	sw := make([]int, 0, slaveCount-1)
	for i = 0; i < slaveCount; i++ {
		if n.Slave[i].addr != addr {
			s = append(s, n.Slave[i])
			sw = append(sw, n.SlaveWeights[i])
		}
	}

	n.Slave = s
	n.SlaveWeights = sw
	n.InitBalancer()
	return nil
}

func (n *Node) OpenDB(addr string) (*DB, error) {
	db, err := Open(addr, n.Cfg.User, n.Cfg.Password, "", n.Cfg.MaxConnNum)
	return db, err
}

// 创建一个UpDB, 不一定能用
func (n *Node) UpDB(addr string) (*DB, error) {
	db, err := n.OpenDB(addr)

	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		atomic.StoreInt32(&(db.state), Down)
		return nil, err
	}
	atomic.StoreInt32(&(db.state), Up)
	return db, nil
}

// 创建一个UpDB, 不一定能用
func (n *Node) UpMaster(addr string) error {
	db, err := n.UpDB(addr)
	if err != nil {
		log.ErrorErrorf(err, "Node UpMaster")
	}
	n.Master = db
	return err
}

func (n *Node) UpSlave(addr string) error {
	db, err := n.UpDB(addr)
	if err != nil {
		log.ErrorErrorf(err, "Node UpSlave")
	}

	// 写锁定
	n.Lock()
	for k, slave := range n.Slave {
		if slave.addr == addr {
			n.Slave[k] = db // 直接替换对应的DB
			n.Unlock()
			return nil
		}
	}

	// 否则添加一个DB
	n.Slave = append(n.Slave, db)
	n.Unlock()

	return err
}

func (n *Node) DownMaster(addr string, state int32) error {
	db := n.Master
	if db == nil || db.addr != addr {
		return errors.ErrNoMasterDB
	}

	// 关闭Master, 设置状态
	db.Close()
	atomic.StoreInt32(&(db.state), state)
	return nil
}

func (n *Node) DownSlave(addr string, state int32) error {
	n.RLock()
	if n.Slave == nil {
		n.RUnlock()
		return errors.ErrNoSlaveDB
	}
	slaves := make([]*DB, len(n.Slave))
	copy(slaves, n.Slave)
	n.RUnlock()

	//slave is *DB
	// TODO: Slave挂了之后，如何做Balance呢?
	for _, slave := range slaves {
		if slave.addr == addr {
			// 关闭对应的Slave? 然后呢设置状态
			slave.Close()
			atomic.StoreInt32(&(slave.state), state)
			break
		}
	}
	return nil
}

func (n *Node) ParseMaster(masterStr string) error {
	var err error
	if len(masterStr) == 0 {
		return errors.ErrNoMasterDB
	}

	n.Master, err = n.OpenDB(masterStr)
	return err
}

//slaveStr(127.0.0.1:3306@2,192.168.0.12:3306@3)
func (n *Node) ParseSlave(slaveStr string) error {
	var db *DB
	var weight int
	var err error

	if len(slaveStr) == 0 {
		return nil
	}
	// 逗号分隔的字符串
	slaveStr = strings.Trim(slaveStr, SlaveSplit)
	slaveArray := strings.Split(slaveStr, SlaveSplit)
	count := len(slaveArray)
	n.Slave = make([]*DB, 0, count)
	n.SlaveWeights = make([]int, 0, count)

	//parse addr and weight
	for i := 0; i < count; i++ {
		addrAndWeight := strings.Split(slaveArray[i], WeightSplit)
		if len(addrAndWeight) == 2 {
			weight, err = strconv.Atoi(addrAndWeight[1])
			if err != nil {
				return err
			}
		} else {
			weight = 1
		}
		n.SlaveWeights = append(n.SlaveWeights, weight)
		if db, err = n.OpenDB(addrAndWeight[0]); err != nil {
			return err
		}
		n.Slave = append(n.Slave, db)
	}
	n.InitBalancer()
	return nil
}

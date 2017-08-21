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

	"github.com/flike/kingshard/config"
	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
)

const (
	Master      = "master"
	Slave       = "slave"
	SlaveSplit  = ","
	WeightSplit = "@"
)

type Node struct {
	Cfg config.NodeConfig

	sync.RWMutex
	Master *DB

	/* 从实例数组 */
	Slave          []*DB
	LastSlaveIndex int
	/* rr列表 */
	RoundRobinQ  []int
	SlaveWeights []int

	DownAfterNoAlive time.Duration
}

//节点后端mysql实例存活检测
func (n *Node) CheckNode() {
	//to do
	//1 check connection alive
	for {
		n.checkMaster()
		n.checkSlave()
		/* 每间隔16秒检查一次 */
		time.Sleep(16 * time.Second)
	}
}

func (n *Node) String() string {
	return n.Cfg.Name
}

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

func (n *Node) GetSlaveConn() (*BackendConn, error) {
	n.Lock()
	db, err := n.GetNextSlave()
	n.Unlock()
	if err != nil {
		return nil, err
	}

	if db == nil {
		return nil, errors.ErrNoSlaveDB
	}
	if atomic.LoadInt32(&(db.state)) == Down {
		return nil, errors.ErrSlaveDown
	}

	return db.GetConn()
}

//检查master实例是否存活,如果实例已经失去连接超过固定时间，则将master状态置为Down
//如果master可以ping通，则将master重新上线，状态置为up
func (n *Node) checkMaster() {
	db := n.Master
	if db == nil {
		golog.Error("Node", "checkMaster", "Master is no alive", 0)
		return
	}

	if err := db.Ping(); err != nil {
		golog.Error("Node", "checkMaster", "Ping", 0, "db.Addr", db.Addr(), "error", err.Error())
	} else {
		if atomic.LoadInt32(&(db.state)) == Down {
			golog.Info("Node", "checkMaster", "Master up", 0, "db.Addr", db.Addr())
			n.UpMaster(db.addr)
		}
		db.SetLastPing()
		if atomic.LoadInt32(&(db.state)) != ManualDown {
			atomic.StoreInt32(&(db.state), Up)
		}
		return
	}

	if int64(n.DownAfterNoAlive) > 0 && time.Now().Unix()-db.GetLastPing() > int64(n.DownAfterNoAlive/time.Second) {
		golog.Info("Node", "checkMaster", "Master down", 0,
			"db.Addr", db.Addr(),
			"Master_down_time", int64(n.DownAfterNoAlive/time.Second))
		n.DownMaster(db.addr, Down)
	}
}

//检查所有的从实例是否存活,如果从实例未检测超过一定时间，则将从实例下线
//TODO 下线后不需要维护rr队列吗?
func (n *Node) checkSlave() {
	/* TODO 为什么加读锁? */
	n.RLock()
	if n.Slave == nil {
		n.RUnlock()
		return
	}
	slaves := make([]*DB, len(n.Slave))
	copy(slaves, n.Slave)
	n.RUnlock()

	for i := 0; i < len(slaves); i++ {
		if err := slaves[i].Ping(); err != nil {
			golog.Error("Node", "checkSlave", "Ping", 0, "db.Addr", slaves[i].Addr(), "error", err.Error())
		} else {
			if atomic.LoadInt32(&(slaves[i].state)) == Down {
				golog.Info("Node", "checkSlave", "Slave up", 0, "db.Addr", slaves[i].Addr())
				n.UpSlave(slaves[i].addr)
			}
			slaves[i].SetLastPing()
			if atomic.LoadInt32(&(slaves[i].state)) != ManualDown {
				atomic.StoreInt32(&(slaves[i].state), Up)
			}
			continue
		}

		if int64(n.DownAfterNoAlive) > 0 && time.Now().Unix()-slaves[i].GetLastPing() > int64(n.DownAfterNoAlive/time.Second) {
			golog.Info("Node", "checkSlave", "Slave down", 0,
				"db.Addr", slaves[i].Addr(),
				"slave_down_time", int64(n.DownAfterNoAlive/time.Second))
			//If can't ping slave after DownAfterNoAlive, set slave Down
			n.DownSlave(slaves[i].addr, Down)
		}
	}

}

//添加从实例，建立一组连接
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
	/* 初始DB为空 */
	db, err := Open(addr, n.Cfg.User, n.Cfg.Password, "", n.Cfg.MaxConnNum)
	return db, err
}

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

func (n *Node) UpMaster(addr string) error {
	db, err := n.UpDB(addr)
	if err != nil {
		golog.Error("Node", "UpMaster", err.Error(), 0)
	}
	n.Master = db
	return err
}

func (n *Node) UpSlave(addr string) error {
	db, err := n.UpDB(addr)
	if err != nil {
		golog.Error("Node", "UpSlave", err.Error(), 0)
	}

	n.Lock()
	for k, slave := range n.Slave {
		if slave.addr == addr {
			n.Slave[k] = db
			n.Unlock()
			return nil
		}
	}
	n.Slave = append(n.Slave, db)
	n.Unlock()

	return err
}

func (n *Node) DownMaster(addr string, state int32) error {
	db := n.Master
	if db == nil || db.addr != addr {
		return errors.ErrNoMasterDB
	}

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
	for _, slave := range slaves {
		if slave.addr == addr {
			slave.Close()
			atomic.StoreInt32(&(slave.state), state)
			break
		}
	}
	return nil
}

//构造master实例，建立master连接池
func (n *Node) ParseMaster(masterStr string) error {
	var err error
	if len(masterStr) == 0 {
		return errors.ErrNoMasterDB
	}

	n.Master, err = n.OpenDB(masterStr)
	return err
}

//slaveStr(127.0.0.1:3306@2,192.168.0.12:3306@3)
//构造slave数组，建立各个slave实例连接池
func (n *Node) ParseSlave(slaveStr string) error {
	var db *DB
	var weight int
	var err error

	if len(slaveStr) == 0 {
		return nil
	}
	/* 去除头尾的SlaveSplit字符串 */
	slaveStr = strings.Trim(slaveStr, SlaveSplit)
	slaveArray := strings.Split(slaveStr, SlaveSplit)
	count := len(slaveArray)
	/* 从库数组 */
	n.Slave = make([]*DB, 0, count)
	/* 从库权重数组 */
	n.SlaveWeights = make([]int, 0, count)

	//parse addr and weight
	for i := 0; i < count; i++ {
		/* 分割addr和权重 */
		addrAndWeight := strings.Split(slaveArray[i], WeightSplit)
		if len(addrAndWeight) == 2 {
			weight, err = strconv.Atoi(addrAndWeight[1])
			if err != nil {
				return err
			}
		} else {
			/* 如果没有权重，默认为1 */
			weight = 1
		}
		n.SlaveWeights = append(n.SlaveWeights, weight)
		/* 根据addr信息建立从库缓存和空闲连接池 */
		if db, err = n.OpenDB(addrAndWeight[0]); err != nil {
			return err
		}
		n.Slave = append(n.Slave, db)
	}
	/* 初始化slave权重轮询负载均衡机制 */
	n.InitBalancer()
	return nil
}

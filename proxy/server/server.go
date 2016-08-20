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

package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/flike/kingshard/mysql"

	"github.com/flike/kingshard/backend"
	"github.com/flike/kingshard/config"
	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/proxy/router"
)

type Schema struct {
	db string

	nodes map[string]*backend.Node

	rule *router.Router
}

type BlacklistSqls struct {
	sqls    map[string]string
	sqlsLen int
}

const (
	Offline = iota
	Online
	Unknown
)

type Server struct {
	cfg      *config.Config
	addr     string
	user     string
	password string
	db       string

	statusIndex        int32
	status             [2]int32
	logSqlIndex        int32
	logSql             [2]string
	slowLogTimeIndex   int32
	slowLogTime        [2]int
	blacklistSqlsIndex int32
	blacklistSqls      [2]*BlacklistSqls
	allowipsIndex      int32
	allowips           [2][]net.IP

	counter *Counter
	nodes   map[string]*backend.Node
	schema  *Schema

	listener net.Listener

	/* 运行标示 */
	running bool
}

func (s *Server) Status() string {
	var status string
	switch s.status[s.statusIndex] {
	case Online:
		status = "online"
	case Offline:
		status = "offline"
	case Unknown:
		status = "unknown"
	default:
		status = "unknown"
	}
	return status
}

//TODO
func (s *Server) parseAllowIps() error {
	atomic.StoreInt32(&s.allowipsIndex, 0)
	cfg := s.cfg
	if len(cfg.AllowIps) == 0 {
		return nil
	}
	ipVec := strings.Split(cfg.AllowIps, ",")
	s.allowips[s.allowipsIndex] = make([]net.IP, 0, 10)
	s.allowips[1] = make([]net.IP, 0, 10)
	for _, ip := range ipVec {
		s.allowips[s.allowipsIndex] = append(s.allowips[s.allowipsIndex], net.ParseIP(strings.TrimSpace(ip)))
	}
	return nil
}

//TODO parse the blacklist sql file
func (s *Server) parseBlackListSqls() error {
	bs := new(BlacklistSqls)
	bs.sqls = make(map[string]string)
	if len(s.cfg.BlsFile) != 0 {
		file, err := os.Open(s.cfg.BlsFile)
		if err != nil {
			return err
		}

		defer file.Close()
		rd := bufio.NewReader(file)
		for {
			line, err := rd.ReadString('\n')
			//end of file
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			line = strings.TrimSpace(line)
			if len(line) != 0 {
				fingerPrint := mysql.GetFingerprint(line)
				md5 := mysql.GetMd5(fingerPrint)
				bs.sqls[md5] = fingerPrint
			}
		}
	}
	bs.sqlsLen = len(bs.sqls)
	atomic.StoreInt32(&s.blacklistSqlsIndex, 0)
	s.blacklistSqls[s.blacklistSqlsIndex] = bs
	s.blacklistSqls[1] = bs

	return nil
}

func (s *Server) parseNode(cfg config.NodeConfig) (*backend.Node, error) {
	var err error
	n := new(backend.Node)
	/* 节点配置 */
	n.Cfg = cfg

	/* 标记节点下线的间隔时间 */
	n.DownAfterNoAlive = time.Duration(cfg.DownAfterNoAlive) * time.Second
	/* 建立node节点内ks与主实例连接池并初始化DB状态为Unknown */
	err = n.ParseMaster(cfg.Master)
	if err != nil {
		return nil, err
	}

	/* TODO 建立ks与从实例连接池,初始化round robin队列 */
	err = n.ParseSlave(cfg.Slave)
	if err != nil {
		return nil, err
	}

	/* 一个node对应一个goroutine进行节点内多实例存活检测 */
	go n.CheckNode()

	return n, nil
}

func (s *Server) parseNodes() error {
	cfg := s.cfg
	/* 以nodes数组初始化node字典,字典的key为node中的name */
	s.nodes = make(map[string]*backend.Node, len(cfg.Nodes))

	for _, v := range cfg.Nodes {
		/* 不允许有同名node */
		if _, ok := s.nodes[v.Name]; ok {
			return fmt.Errorf("duplicate node [%s].", v.Name)
		}

		/* 解析节点集内的节点，建立ks与节点内实例的连接池，建立节点保活goroutine */
		n, err := s.parseNode(v)
		if err != nil {
			return err
		}

		/* node字典赋值 */
		s.nodes[v.Name] = n
	}

	return nil
}

func (s *Server) parseSchema() error {
	schemaCfg := s.cfg.Schema
	if len(schemaCfg.Nodes) == 0 {
		return fmt.Errorf("schema [%s] must have a node.", schemaCfg.DB)
	}

	nodes := make(map[string]*backend.Node)
	for _, n := range schemaCfg.Nodes {
		/* 检查节点是否存在 */
		if s.GetNode(n) == nil {
			return fmt.Errorf("schema [%s] node [%s] config is not exists.", schemaCfg.DB, n)
		}

		/* 检查node数组里面是否存在该节点 */
		if _, ok := nodes[n]; ok {
			return fmt.Errorf("schema [%s] node [%s] duplicate.", schemaCfg.DB, n)
		}

		/* 节点集map赋值 */
		nodes[n] = s.GetNode(n)
	}

	//解析分表规则
	rule, err := router.NewRouter(&schemaCfg)
	if err != nil {
		return err
	}

	s.schema = &Schema{
		db:    schemaCfg.DB,
		nodes: nodes,
		rule:  rule,
	}
	/* TODO server端db的作用? */
	s.db = schemaCfg.DB

	return nil
}

func NewServer(cfg *config.Config) (*Server, error) {
	/* new关键字返回指针 */
	s := new(Server)

	s.cfg = cfg
	s.counter = new(Counter)
	s.addr = cfg.Addr
	s.user = cfg.User
	s.password = cfg.Password
	atomic.StoreInt32(&s.statusIndex, 0)
	s.status[s.statusIndex] = Online
	atomic.StoreInt32(&s.logSqlIndex, 0)
	s.logSql[s.logSqlIndex] = cfg.LogSql
	atomic.StoreInt32(&s.slowLogTimeIndex, 0)
	s.slowLogTime[s.slowLogTimeIndex] = cfg.SlowLogTime
	if len(cfg.Charset) != 0 {
		cid, ok := mysql.CharsetIds[cfg.Charset]
		if !ok {
			return nil, errors.ErrInvalidCharset
		}
		//change the default charset
		mysql.DEFAULT_CHARSET = cfg.Charset
		mysql.DEFAULT_COLLATION_ID = cid
		mysql.DEFAULT_COLLATION_NAME = mysql.Collations[cid]
	}

	//黑名单sql
	if err := s.parseBlackListSqls(); err != nil {
		return nil, err
	}

	/* 加载ip白名单 */
	if err := s.parseAllowIps(); err != nil {
		return nil, err
	}

	/* 解析节点信息,建立ks与后端实例的连接池,此处不需要指定DB */
	if err := s.parseNodes(); err != nil {
		return nil, err
	}

	/* 加载schema信息 */
	/* 加载分表规则 */
	if err := s.parseSchema(); err != nil {
		return nil, err
	}

	var err error
	netProto := "tcp"

	s.listener, err = net.Listen(netProto, s.addr)

	if err != nil {
		return nil, err
	}

	golog.Info("server", "NewServer", "Server running", 0,
		"netProto",
		netProto,
		"address",
		s.addr)
	return s, nil
}

func (s *Server) flushCounter() {
	for {
		s.counter.FlushCounter()
		time.Sleep(1 * time.Second)
	}
}

func (s *Server) newClientConn(co net.Conn) *ClientConn {
	c := new(ClientConn)
	tcpConn := co.(*net.TCPConn)

	//SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that data is sent as soon as possible after a Write.
	//I set this option false.
	tcpConn.SetNoDelay(false)
	c.c = tcpConn

	/* 将服务端schema赋值给客户端,为分表做准备 */
	//TODO 客户端不需要记录schema信息
	c.schema = s.GetSchema()

	/* mysql通讯协议结构 */
	c.pkg = mysql.NewPacketIO(tcpConn)
	/* 每一个连接的服务结构体 */
	c.proxy = s
	/* 命令序列号 */
	c.pkg.Sequence = 0
	/* 连接标识ID */
	c.connectionId = atomic.AddUint32(&baseConnId, 1)
	/* 每一个连接的状态，标识是否处在事物中 */
	c.status = mysql.SERVER_STATUS_AUTOCOMMIT
	/* 根据时间生成随机盐 */
	c.salt, _ = mysql.RandomBuf(20)
	/* node结构指针为key，后端连接结构指针为value */
	c.txConns = make(map[*backend.Node]*backend.BackendConn)
	/* 连接是否关闭 */
	c.closed = false
	/* 连接编码 */
	c.charset = mysql.DEFAULT_CHARSET
	/* 校对值 */
	c.collation = mysql.DEFAULT_COLLATION_ID
	/* 预编译ID */
	c.stmtId = 0
	/* 预编译语句结构体 */
	c.stmts = make(map[uint32]*Stmt)

	return c
}

func (s *Server) onConn(c net.Conn) {
	s.counter.IncrClientConns()
	conn := s.newClientConn(c) //新建一个conn

	defer func() {
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] //获得当前goroutine的stacktrace
			golog.Error("server", "onConn", "error", 0,
				"remoteAddr", c.RemoteAddr().String(),
				"stack", string(buf),
			)
		}

		conn.Close()
		s.counter.DecrClientConns()
	}()

	/* 判断ip是否在白名单中 */
	if allowConnect := conn.IsAllowConnect(); allowConnect == false {
		err := mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR, "ip address access denied by kingshard.")
		conn.writeError(err)
		conn.Close()
		return
	}

	/* 握手认证阶段*/
	// 服务器 -> 客户端：握手初始化消息
	// 客户端 -> 服务器：登陆认证消息
	// 服务器 -> 客户端：认证结果消息

	/* 命令执行阶段 */
	//客户端 -> 服务器：执行命令消息
	// 服务器 -> 客户端：命令执行结果

	/* mysql握手协议,先要求应用端上送用户名、密码然后校验ks用户名、密码并返回校验结果，握手成功，执行后续命令 */
	if err := conn.Handshake(); err != nil {
		golog.Error("server", "onConn", err.Error(), 0)
		c.Close()
		return
	}

	conn.Run()
}

func (s *Server) changeProxy(v string) error {
	var status int32
	switch v {
	case "online":
		status = Online
	case "offline":
		status = Offline
	default:
		status = Unknown
	}
	if status == Unknown {
		return errors.ErrCmdUnsupport
	}

	if s.statusIndex == 0 {
		s.status[1] = status
		atomic.StoreInt32(&s.statusIndex, 1)
	} else {
		s.status[0] = status
		atomic.StoreInt32(&s.statusIndex, 0)
	}

	return nil
}

func (s *Server) changeLogSql(v string) error {
	if s.logSqlIndex == 0 {
		s.logSql[1] = v
		atomic.StoreInt32(&s.logSqlIndex, 1)
	} else {
		s.logSql[0] = v
		atomic.StoreInt32(&s.logSqlIndex, 0)
	}
	s.cfg.LogSql = v

	return nil
}

func (s *Server) changeSlowLogTime(v string) error {
	tmp, err := strconv.Atoi(v)
	if err != nil {
		return err
	}

	if s.slowLogTimeIndex == 0 {
		s.slowLogTime[1] = tmp
		atomic.StoreInt32(&s.slowLogTimeIndex, 1)
	} else {
		s.slowLogTime[0] = tmp
		atomic.StoreInt32(&s.slowLogTimeIndex, 0)
	}
	s.cfg.SlowLogTime = tmp

	return err
}

func (s *Server) addAllowIP(v string) error {
	clientIP := net.ParseIP(v)

	for _, ip := range s.allowips[s.allowipsIndex] {
		if ip.Equal(clientIP) {
			return nil
		}
	}

	if s.allowipsIndex == 0 {
		s.allowips[1] = s.allowips[0]
		s.allowips[1] = append(s.allowips[1], clientIP)
		atomic.StoreInt32(&s.allowipsIndex, 1)
	} else {
		s.allowips[0] = s.allowips[1]
		s.allowips[0] = append(s.allowips[0], clientIP)
		atomic.StoreInt32(&s.allowipsIndex, 0)
	}

	if s.cfg.AllowIps == "" {
		s.cfg.AllowIps = strings.Join([]string{s.cfg.AllowIps, v}, "")
	} else {
		s.cfg.AllowIps = strings.Join([]string{s.cfg.AllowIps, v}, ",")
	}

	return nil
}

func (s *Server) delAllowIP(v string) error {
	clientIP := net.ParseIP(v)

	if s.allowipsIndex == 0 {
		s.allowips[1] = s.allowips[0]
		ipVec2 := strings.Split(s.cfg.AllowIps, ",")
		for i, ip := range s.allowips[1] {
			if ip.Equal(clientIP) {
				s.allowips[1] = append(s.allowips[1][:i], s.allowips[1][i+1:]...)
				atomic.StoreInt32(&s.allowipsIndex, 1)
				for i, ip := range ipVec2 {
					if ip == v {
						ipVec2 = append(ipVec2[:i], ipVec2[i+1:]...)
						s.cfg.AllowIps = strings.Trim(strings.Join(ipVec2, ","), ",")
						return nil
					}
				}
				return nil
			}
		}
	} else {
		s.allowips[0] = s.allowips[1]
		ipVec2 := strings.Split(s.cfg.AllowIps, ",")
		for i, ip := range s.allowips[0] {
			if ip.Equal(clientIP) {
				s.allowips[0] = append(s.allowips[0][:i], s.allowips[0][i+1:]...)
				atomic.StoreInt32(&s.allowipsIndex, 0)
				for i, ip := range ipVec2 {
					if ip == v {
						ipVec2 = append(ipVec2[:i], ipVec2[i+1:]...)
						s.cfg.AllowIps = strings.Trim(strings.Join(ipVec2, ","), ",")
						return nil
					}
				}
				return nil
			}
		}
	}

	return nil
}

func (s *Server) addBlackSql(v string) error {
	v = strings.TrimSpace(v)
	fingerPrint := mysql.GetFingerprint(v)
	md5 := mysql.GetMd5(fingerPrint)
	if s.blacklistSqlsIndex == 0 {
		s.blacklistSqls[1] = s.blacklistSqls[0]
		s.blacklistSqls[1].sqls[md5] = v
		atomic.StoreInt32(&s.blacklistSqlsIndex, 1)
	} else {
		s.blacklistSqls[0] = s.blacklistSqls[1]
		s.blacklistSqls[0].sqls[md5] = v
		atomic.StoreInt32(&s.blacklistSqlsIndex, 0)
	}

	return nil
}

func (s *Server) delBlackSql(v string) error {
	v = strings.TrimSpace(v)
	fingerPrint := mysql.GetFingerprint(v)
	md5 := mysql.GetMd5(fingerPrint)

	if s.blacklistSqlsIndex == 0 {
		s.blacklistSqls[1] = s.blacklistSqls[0]
		s.blacklistSqls[1].sqls[md5] = v
		delete(s.blacklistSqls[1].sqls, md5)
		atomic.StoreInt32(&s.blacklistSqlsIndex, 1)
	} else {
		s.blacklistSqls[0] = s.blacklistSqls[1]
		s.blacklistSqls[0].sqls[md5] = v
		delete(s.blacklistSqls[0].sqls, md5)
		atomic.StoreInt32(&s.blacklistSqlsIndex, 0)
	}

	return nil
}

func (s *Server) saveBlackSql() error {
	if len(s.cfg.BlsFile) == 0 {
		return nil
	}
	f, err := os.Create(s.cfg.BlsFile)
	if err != nil {
		golog.Error("Server", "saveBlackSql", "create file error", 0,
			"err", err.Error(),
			"blacklist_sql_file", s.cfg.BlsFile,
		)
		return err
	}

	for _, v := range s.blacklistSqls[s.blacklistSqlsIndex].sqls {
		v = v + "\n"
		_, err = f.WriteString(v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) handleSaveProxyConfig() error {
	err := config.WriteConfigFile(s.cfg)
	if err != nil {
		return err
	}

	err = s.saveBlackSql()
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) Run() error {
	s.running = true

	/* 没有收到退出信号时 */
	// flush counter
	go s.flushCounter()

	for s.running {
		/* 主进程监听端口，接收请求,常驻程序阻塞点 */
		conn, err := s.listener.Accept()
		if err != nil {
			golog.Error("server", "Run", err.Error(), 0)
			continue
		}

		/* accept每个请求后用单独的goroutine处理请求 */
		go s.onConn(conn)
	}

	return nil
}

func (s *Server) Close() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) DeleteSlave(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.DeleteSlave(addr)
}

func (s *Server) AddSlave(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.AddSlave(addr)
}

func (s *Server) UpMaster(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.UpMaster(addr)
}

func (s *Server) UpSlave(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.UpSlave(addr)
}

func (s *Server) DownMaster(node, masterAddr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}
	return n.DownMaster(masterAddr, backend.ManualDown)
}

func (s *Server) DownSlave(node, slaveAddr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node [%s].", node)
	}
	return n.DownSlave(slaveAddr, backend.ManualDown)
}

func (s *Server) GetNode(name string) *backend.Node {
	return s.nodes[name]
}

func (s *Server) GetSchema() *Schema {
	return s.schema
}

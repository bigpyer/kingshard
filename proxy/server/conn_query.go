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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/flike/kingshard/backend"
	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/core/hack"
	"github.com/flike/kingshard/mysql"
	"github.com/flike/kingshard/proxy/router"
	"github.com/flike/kingshard/sqlparser"
)

/*处理query语句*/
func (c *ClientConn) handleQuery(sql string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			golog.OutputSql("Error", "err:%v,sql:%s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				golog.Error("ClientConn", "handleQuery",
					err.Error(), 0,
					"stack", string(buf), "sql", sql)
			}
			return
		}
	}()
	/* 删除sql语句最后的分号 */
	sql = strings.TrimRight(sql, ";")

	/* 是否需要分表,如果不需要分表直接进行处理 */
	hasHandled, err := c.preHandleShard(sql)
	if err != nil {
		golog.Error("server", "preHandleShard", err.Error(), 0,
			"sql", sql,
			"hasHandled", hasHandled,
		)
		return err
	}
	/* 如果是不需要分表且已经处理完成，不再执行下面的动作 */
	if hasHandled {
		return nil
	}

	var stmt sqlparser.Statement

	/* 需要分表或是自定义sql语句，则需要我们自行解析sql语句 */
	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		golog.Error("server", "parse", err.Error(), 0, "hasHandled", hasHandled, "sql", sql)
		return err
	}

	/* 类型查询 */
	switch v := stmt.(type) {
	/* 处理SELECT */
	case *sqlparser.Select:
		return c.handleSelect(v, nil)
	/* 处理INSERT */
	case *sqlparser.Insert:
		return c.handleExec(stmt, nil)
	/* 处理UPDATE */
	case *sqlparser.Update:
		return c.handleExec(stmt, nil)
	/* 处理DELETE */
	case *sqlparser.Delete:
		return c.handleExec(stmt, nil)
	/* 处理REPLACE语句非函数 */
	case *sqlparser.Replace:
		return c.handleExec(stmt, nil)
	/* 处理SET语句 */
	case *sqlparser.Set:
		return c.handleSet(v, sql)
	/* 处理BEGIN语句 */
	case *sqlparser.Begin:
		return c.handleBegin()
	/* 处理COMMIT语句 */
	case *sqlparser.Commit:
		return c.handleCommit()
	/* 处理ROLLBACK语句 */
	case *sqlparser.Rollback:
		return c.handleRollback()
	/* 处理管理语句 */
	case *sqlparser.Admin:
		return c.handleAdmin(v)
	/* 管理帮助 */
	case *sqlparser.AdminHelp:
		return c.handleAdminHelp(v)
	/* use db */
	case *sqlparser.UseDB:
		return c.handleUseDB(v.DB)
	/* 简单查询，目前是select last_insert_id() */
	case *sqlparser.SimpleSelect:
		return c.handleSimpleSelect(v)
	default:
		return fmt.Errorf("statement %T not support now", stmt)
	}

	return nil
}

/* TODO 每一次获取连接的时候都需要设置DB、编码等，因为连接是复用的,但是这样会增加两次网络交互，因为ks是对应一个schema的，可不可以提前初始化好连接的db信息呢 */
func (c *ClientConn) getBackendConn(n *backend.Node, fromSlave bool) (co *backend.BackendConn, err error) {
	/* 如果没有处于事务中 */
	if !c.isInTransaction() {
		/* 如果从库标识为真 */
		if fromSlave {
			/* 根据round robin获取一个从实例连接 */
			/* 如果从实例状态为Down，则走主实例 */
			co, err = n.GetSlaveConn()
			if err != nil {
				co, err = n.GetMasterConn()
			}
		} else {
			/* 如果非读从，则走主 */
			co, err = n.GetMasterConn()
		}
		if err != nil {
			golog.Error("server", "getBackendConn", err.Error(), 0)
			return
		}
	} else {
		/* 如果处于事务中,后端连接已经确定 */
		var ok bool
		co, ok = c.txConns[n]

		//第一次获取事务连接
		if !ok {
			if co, err = n.GetMasterConn(); err != nil {
				return
			}

			if !c.isAutoCommit() { //通过set autocommit = 0开启事务
				if err = co.SetAutoCommit(0); err != nil {
					return
				}
			} else { //通过begin开启事务
				if err = co.Begin(); err != nil {
					return
				}
			}

			c.txConns[n] = co
		}
	}
	//todo, set conn charset, etc...
	/* 设置后端连接数据库 */
	if err = co.UseDB(c.db); err != nil {
		return
	}

	/* 设置后端连接字符集 */
	if err = co.SetCharset(c.charset); err != nil {
		return
	}

	return
}

//获取shard的conn，第一个参数表示是不是select
func (c *ClientConn) getShardConns(fromSlave bool, plan *router.Plan) (map[string]*backend.BackendConn, error) {
	var err error
	if plan == nil || len(plan.RouteNodeIndexs) == 0 {
		return nil, errors.ErrNoRouteNode
	}

	nodesCount := len(plan.RouteNodeIndexs)
	nodes := make([]*backend.Node, 0, nodesCount)
	for i := 0; i < nodesCount; i++ {
		nodeIndex := plan.RouteNodeIndexs[i]
		nodes = append(nodes, c.proxy.GetNode(plan.Rule.Nodes[nodeIndex]))
	}
	if c.isInTransaction() {
		if 1 < len(nodes) {
			return nil, errors.ErrTransInMulti
		}
		//exec in multi node
		if len(c.txConns) == 1 && c.txConns[nodes[0]] == nil {
			return nil, errors.ErrTransInMulti
		}
	}
	conns := make(map[string]*backend.BackendConn)
	var co *backend.BackendConn
	for _, n := range nodes {
		co, err = c.getBackendConn(n, fromSlave)
		if err != nil {
			break
		}

		conns[n.Cfg.Name] = co
	}

	return conns, err
}

func (c *ClientConn) executeInNode(conn *backend.BackendConn, sql string, args []interface{}) ([]*mysql.Result, error) {
	var state string

	startTime := time.Now().UnixNano()
	/* 根据mysql通讯协议，向后端传输sql语句或者prepare的方式，并获取执行结果 */
	r, err := conn.Execute(sql, args...)
	if err != nil {
		state = "ERROR"
	} else {
		state = "OK"
	}
	execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
	if strings.ToLower(c.proxy.logSql[c.proxy.logSqlIndex]) != golog.LogSqlOff &&
		execTime > float64(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex]) {
		c.proxy.counter.IncrSlowLogTotal()
		golog.OutputSql(state, "%.1fms - %s->%s:%s",
			execTime,
			c.c.RemoteAddr(),
			conn.GetAddr(),
			sql,
		)
	}

	if err != nil {
		return nil, err
	}

	return []*mysql.Result{r}, err
}

func (c *ClientConn) executeInMultiNodes(conns map[string]*backend.BackendConn, sqls map[string][]string, args []interface{}) ([]*mysql.Result, error) {
	if len(conns) != len(sqls) {
		golog.Error("ClientConn", "executeInMultiNodes", errors.ErrConnNotEqual.Error(), c.connectionId,
			"conns", conns,
			"sqls", sqls,
		)
		return nil, errors.ErrConnNotEqual
	}

	var wg sync.WaitGroup

	if len(conns) == 0 {
		return nil, errors.ErrNoPlan
	}

	wg.Add(len(conns))

	resultCount := 0
	for _, sqlSlice := range sqls {
		resultCount += len(sqlSlice)
	}

	rs := make([]interface{}, resultCount)

	f := func(rs []interface{}, i int, execSqls []string, co *backend.BackendConn) {
		var state string
		for _, v := range execSqls {
			startTime := time.Now().UnixNano()
			r, err := co.Execute(v, args...)
			if err != nil {
				state = "ERROR"
				rs[i] = err
			} else {
				state = "OK"
				rs[i] = r
			}
			execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
			if c.proxy.logSql[c.proxy.logSqlIndex] != golog.LogSqlOff &&
				execTime > float64(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex]) {
				c.proxy.counter.IncrSlowLogTotal()
				golog.OutputSql(state, "%.1fms - %s->%s:%s",
					execTime,
					c.c.RemoteAddr(),
					co.GetAddr(),
					v,
				)
			}
			i++
		}
		wg.Done()
	}

	offsert := 0
	for nodeName, co := range conns {
		s := sqls[nodeName] //[]string
		go f(rs, offsert, s, co)
		offsert += len(s)
	}

	wg.Wait()

	var err error
	r := make([]*mysql.Result, resultCount)
	for i, v := range rs {
		if e, ok := v.(error); ok {
			err = e
			break
		}
		r[i] = rs[i].(*mysql.Result)
	}

	return r, err
}

func (c *ClientConn) closeConn(conn *backend.BackendConn, rollback bool) {
	if c.isInTransaction() {
		return
	}

	if rollback {
		conn.Rollback()
	}

	/* 回收连接 */
	conn.Close()
}

func (c *ClientConn) closeShardConns(conns map[string]*backend.BackendConn, rollback bool) {
	if c.isInTransaction() {
		return
	}

	for _, co := range conns {
		if rollback {
			co.Rollback()
		}
		co.Close()
	}
}

func (c *ClientConn) newEmptyResultset(stmt *sqlparser.Select) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(stmt.SelectExprs))

	for i, expr := range stmt.SelectExprs {
		r.Fields[i] = &mysql.Field{}
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			r.Fields[i].Name = []byte("*")
		case *sqlparser.NonStarExpr:
			if e.As != nil {
				r.Fields[i].Name = e.As
				r.Fields[i].OrgName = hack.Slice(nstring(e.Expr))
			} else {
				r.Fields[i].Name = hack.Slice(nstring(e.Expr))
			}
		default:
			r.Fields[i].Name = hack.Slice(nstring(e))
		}
	}

	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}

/* 事务型语句获取节点(事务不会垮节点，也不会有主从?) */
func (c *ClientConn) GetTransExecNode(tokens []string, sql string) (*backend.Node, error) {
	var execNode *backend.Node
	var err error

	/* 根据事务语句的节点注释获取node名称 */
	tokensLen := len(tokens)
	if 2 <= tokensLen {
		/* 如果存在注释 */
		if tokens[0][0] == mysql.COMMENT_PREFIX {
			nodeName := strings.Trim(tokens[0], mysql.COMMENT_STRING)
			/* 校验传输的node在schema中nodes里是否有配置 */
			if c.schema.nodes[nodeName] != nil {
				execNode = c.schema.nodes[nodeName]
			}
		}
	}

	/* 事务语句中不包含节点注释 */
	if execNode == nil {
		execNode, _, err = c.GetExecNode(tokens, sql)
		if err != nil {
			return nil, err
		}
		return execNode, nil
	}
	if len(c.txConns) == 1 && c.txConns[execNode] == nil {
		return nil, errors.ErrTransInMulti
	}
	return execNode, nil
}

/* TODO 这个跟事务型获取node有什么区别呢?根据普通注释获取节点名称，如果语句中没有节点信息，则使用默认节点。 */
func (c *ClientConn) GetExecNode(tokens []string,
	sql string) (*backend.Node, bool, error) {
	var execNode *backend.Node
	var fromSlave bool

	schema := c.proxy.schema
	rules := schema.rule.Rules

	tokensLen := len(tokens)
	if 0 < tokensLen {
		tokenId, ok := mysql.PARSE_TOKEN_MAP[strings.ToLower(tokens[0])]
		if ok == true {
			switch tokenId {
			case mysql.TK_ID_SELECT, mysql.TK_ID_DELETE:
				if len(rules) == 0 {
					if tokenId == mysql.TK_ID_SELECT {
						fromSlave = true
					}
					break
				}
				for i := 1; i < tokensLen; i++ {
					if strings.ToLower(tokens[i]) == mysql.TK_STR_FROM {
						if i+1 < tokensLen {
							tableName := sqlparser.GetTableName(tokens[i+1])

							/* 如果表名称在配置文件中，需要分表，则节点为空,fromSlave为false */
							if _, ok := rules[tableName]; ok {
								return nil, false, nil
							} else {
								if tokenId == mysql.TK_ID_SELECT {
									fromSlave = true
								}
							}
						}
					}
				}
			case mysql.TK_ID_INSERT, mysql.TK_ID_REPLACE:
				if len(rules) == 0 {
					break
				}
				for i := 0; i < tokensLen; i++ {
					if strings.ToLower(tokens[i]) == mysql.TK_STR_INTO {
						if i+1 < tokensLen {
							tableName := sqlparser.GetInsertTableName(tokens[i+1])
							if _, ok := rules[tableName]; ok {
								return nil, false, nil
							}
						}
					}
				}
			case mysql.TK_ID_UPDATE:
				if len(rules) == 0 {
					break
				}
				for i := 0; i < tokensLen; i++ {
					if strings.ToLower(tokens[i]) == mysql.TK_STR_SET {
						tableName := sqlparser.GetTableName(tokens[i-1])
						if _, ok := rules[tableName]; ok {
							return nil, false, nil
						}
					}
				}
			case mysql.TK_ID_SET:
				if len(tokens) < 2 {
					break
				}
				tmp1 := strings.Split(sql, "=")
				tmp2 := strings.Split(tmp1[0], " ")
				secondWord := strings.ToLower(tmp2[1])
				if secondWord == mysql.TK_STR_NAMES ||
					secondWord == mysql.TK_STR_RESULTS ||
					secondWord == mysql.TK_STR_CLIENT ||
					secondWord == mysql.TK_STR_CONNECTION ||
					secondWord == mysql.TK_STR_AUTOCOMMIT {
					return nil, false, nil
				}
			default:
				return nil, false, nil
			}
		}
	}
	//get node
	if 2 <= tokensLen {
		if tokens[0][0] == mysql.COMMENT_PREFIX {
			nodeName := strings.Trim(tokens[0], mysql.COMMENT_STRING)
			if c.schema.nodes[nodeName] != nil {
				execNode = c.schema.nodes[nodeName]
			}
			//select
			if mysql.PARSE_TOKEN_MAP[tokens[1]] == mysql.TK_ID_SELECT {
				fromSlave = true
			}
		}
	}

	/* 如果语句中没有指定node，或者node不存在，采用默认node */
	if execNode == nil {
		defaultRule := c.schema.rule.DefaultRule
		if len(defaultRule.Nodes) == 0 {
			return nil, false, errors.ErrNoDefaultNode
		}
		execNode = c.proxy.GetNode(defaultRule.Nodes[0])
	}

	return execNode, fromSlave, nil
}

//返回true表示sql已经处理，false表示sql未处理
func (c *ClientConn) preHandleShard(sql string) (bool, error) {
	var rs []*mysql.Result
	var err error

	var execNode *backend.Node
	var fromSlave bool = false

	if len(sql) == 0 {
		return false, errors.ErrCmdUnsupport
	}

	tokens := strings.Fields(sql)
	if len(tokens) == 0 {
		return false, errors.ErrCmdUnsupport
	}

	/* 判断是否在事务中 */
	if c.isInTransaction() {
		execNode, err = c.GetTransExecNode(tokens, sql)
	} else {
		/* 如果不是在事务中，需要判断主从 */
		fmt.Println("not in txn")
		execNode, fromSlave, err = c.GetExecNode(tokens, sql)
		fmt.Println(fromSlave)
	}

	if err != nil {
		return false, err
	}
	//need shard sql
	/* 需要跨区执行sql，则返回false，nil，自己实现解析器，解析sql进行处理 */
	if execNode == nil {
		return false, nil
	}

	// TODO execute in Master DB?
	/* 根据上一步获取的node、fromSlave标识，从连接池中获取连接 */
	//execute in Master DB
	conn, err := c.getBackendConn(execNode, fromSlave)
	defer c.closeConn(conn, false)
	if err != nil {
		return false, err
	}

	/* 向连接句柄透传并执行sql语句 */
	rs, err = c.executeInNode(conn, sql, nil)
	if err != nil {
		return false, err
	}

	if len(rs) == 0 {
		msg := fmt.Sprintf("result is empty")
		golog.Error("ClientConn", "handleUnsupport", msg, c.connectionId)
		return false, mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	if rs[0].Resultset != nil {
		/* 获得后端实例应答结果并根据mysql通讯协议，组装应答报文 */
		err = c.writeResultset(c.status, rs[0].Resultset)
	} else {
		err = c.writeOK(rs[0])
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *ClientConn) handleExec(stmt sqlparser.Statement, args []interface{}) error {
	//获取分表执行计划
	plan, err := c.schema.rule.BuildPlan(stmt)
	if err != nil {
		return err
	}
	//根据分表执行计划获取对应的连接,多个节点则返回多个连接
	conns, err := c.getShardConns(false, plan)
	defer c.closeShardConns(conns, err != nil)
	if err != nil {
		golog.Error("ClientConn", "handleExec", err.Error(), c.connectionId)
		return err
	}
	if conns == nil {
		return c.writeOK(nil)
	}

	var rs []*mysql.Result

	//多节点异步执行sql
	rs, err = c.executeInMultiNodes(conns, plan.RewrittenSqls, args)
	if err == nil {
		//汇总执行结果
		err = c.mergeExecResult(rs)
	}

	return err
}

func (c *ClientConn) mergeExecResult(rs []*mysql.Result) error {
	r := new(mysql.Result)
	for _, v := range rs {
		r.Status |= v.Status
		r.AffectedRows += v.AffectedRows
		if r.InsertId == 0 {
			r.InsertId = v.InsertId
		} else if r.InsertId > v.InsertId {
			//last insert id is first gen id for multi row inserted
			//see http://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
			r.InsertId = v.InsertId
		}
	}

	if r.InsertId > 0 {
		c.lastInsertId = int64(r.InsertId)
	}
	c.affectedRows = int64(r.AffectedRows)

	return c.writeOK(r)
}

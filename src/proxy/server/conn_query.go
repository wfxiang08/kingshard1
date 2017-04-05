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

	"backend"
	"core/errors"
	"core/hack"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"mysql"
	"proxy/router"
	"sqlparser"
)

/*处理query语句*/
func (c *ClientConn) handleQuery(sql string) (err error) {
	defer func() {
		// 处理panic等错误，防止崩溃
		if e := recover(); e != nil {
			log.Errorf("err:%v, sql:%s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				log.ErrorErrorf(err, "ClientConn handleQuery stack: %s, sql: %s", string(buf), sql)
			}
			return
		}
	}()

	// 如何执行query呢?
	// 1. 删除SQL语句最后的分号
	sql = strings.TrimRight(sql, ";")

	// 2. 根据SQL进行Shard
	hasHandled, err := c.preHandleShard(sql)
	if err != nil {
		log.ErrorErrorf(err, "server preHandleShard: sql: %s, hasHandled: %t", sql, hasHandled)
		return err
	}
	if hasHandled {
		return nil
	}

	//解析sql语句,得到的stmt是一个interface
	var stmt sqlparser.Statement
	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		log.ErrorErrorf(err, "server parse: sql: %s, hasHandled: %t", sql, hasHandled)
		return err
	}

	switch v := stmt.(type) {
	case *sqlparser.Select:
		return c.handleSelect(v, nil)
	case *sqlparser.Insert:
		return c.handleExec(stmt, nil)
	case *sqlparser.Update:
		return c.handleExec(stmt, nil)
	case *sqlparser.Delete:
		return c.handleExec(stmt, nil)
	case *sqlparser.Replace:
		return c.handleExec(stmt, nil)
	case *sqlparser.Set:
		return c.handleSet(v, sql)
	case *sqlparser.Begin:
		return c.handleBegin()
	case *sqlparser.Commit:
		return c.handleCommit()
	case *sqlparser.Rollback:
		return c.handleRollback()
	case *sqlparser.Admin:
		return c.handleAdmin(v)
	case *sqlparser.AdminHelp:
		return c.handleAdminHelp(v)
	case *sqlparser.UseDB:
		return c.handleUseDB(v.DB)
	case *sqlparser.SimpleSelect:
		return c.handleSimpleSelect(v)
	case *sqlparser.Truncate:
		return c.handleExec(stmt, nil)
	default:
		return fmt.Errorf("statement %T not support now", stmt)
	}

	return nil
}

// 获取获取后端的请求呢?
func (c *ClientConn) getBackendConn(n *backend.Node, fromSlave bool) (co *backend.BackendConn, err error) {
	if !c.isInTransaction() {
		// 不在事务中，尝试获取一个Connection, 优先考虑 Slave, 其次考虑: Master
		if fromSlave {
			co, err = n.GetSlaveConn()
			if err != nil {
				co, err = n.GetMasterConn()
			}
		} else {
			co, err = n.GetMasterConn()
		}
		if err != nil {
			log.ErrorErrorf(err, "server getBackendConn")
			return
		}
	} else {
		// 在事务中，则只能使用指定的node
		var ok bool
		co, ok = c.txConns[n]

		if !ok {
			// 如果没有，也只能获取Master Conn
			if co, err = n.GetMasterConn(); err != nil {
				return
			}

			// 后端的Connection和ClientConn的AutoCommit保持一致
			if !c.isAutoCommit() {
				if err = co.SetAutoCommit(0); err != nil {
					return
				}
			} else {
				// 开始事务
				// c.isAutoCommit()
				if err = co.Begin(); err != nil {
					return
				}
			}

			c.txConns[n] = co
		}
	}

	// 使用指定的DB
	db := c.db
	if len(n.Cfg.DBName) > 0 {
		db = n.Cfg.DBName
	}
	if err = co.UseDB(db); err != nil {
		//reset the database to null
		c.db = ""
		return
	}

	// 设置字符集??
	if err = co.SetCharset(c.charset, c.collation); err != nil {
		return
	}

	return
}

func (c *ClientConn) GetNormalizedDB(db string, n *backend.Node, tableIndex int) string {
	// <db, node> ==> new_db name
	return ""
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

//
// 在指定的Node/backend conn上执行对应的SQL
//
func (c *ClientConn) executeInNode(conn *backend.BackendConn, sql string, args []interface{}) ([]*mysql.Result, error) {
	var state string
	startTime := time.Now().UnixNano()
	r, err := conn.Execute(sql, args...)

	if err != nil {
		state = "ERROR"
	} else {
		state = "OK"
	}
	execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)

	// 处理SQL Query
	if execTime > float64(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex]) {
		c.proxy.counter.IncrSlowLogTotal()

		log.Printf("SLOW State: %s, %.1fms, Remote:%s, SQL: %s", state, execTime,
			c.c.RemoteAddr(),
			sql)
	}

	if err != nil {
		return nil, err
	}

	return []*mysql.Result{r}, err
}

//
// sqls: 其中key为node, []string为同一个node内部的不同表的SQL语句
//
func (c *ClientConn) executeInMultiNodes(conns map[string]*backend.BackendConn, sqls map[string][]string,
	args []interface{}) ([]*mysql.Result, error) {

	if len(conns) != len(sqls) {
		log.ErrorErrorf(errors.ErrConnNotEqual, "ClientConn executeInMultiNodes")
		//golog.Error("ClientConn", "executeInMultiNodes", errors.ErrConnNotEqual.Error(), c.connectionId,
		//	"conns", conns,
		//	"sqls", sqls,
		//)
		return nil, errors.ErrConnNotEqual
	}

	var wg sync.WaitGroup

	if len(conns) == 0 {
		return nil, errors.ErrNoPlan
	}

	wg.Add(len(conns))

	// 统计所有参与查询的表的个数
	resultCount := 0
	for _, sqlSlice := range sqls {
		resultCount += len(sqlSlice)
	}

	rs := make([]interface{}, resultCount)

	// 执行单个查询
	f := func(rs []interface{}, i int, execSqls []string, co *backend.BackendConn) {
		var state string
		// 在同一个node上，执行多个语句（为什么不并发呢？）
		for _, v := range execSqls {
			startTime := time.Now().UnixNano()
			r, err := co.Execute(v, args...)

			// 不同的人写数组的不同部分是ok的
			if err != nil {
				state = "ERROR"
				rs[i] = err
			} else {
				state = "OK"
				rs[i] = r
			}

			// 记录SQL的执行时间：慢查询也可以在这个环节来处理
			execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
			if execTime > float64(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex]) {
				c.proxy.counter.IncrSlowLogTotal()

				log.Printf("SLOW State: %s, %.1fms, Remote:%s, SQL: %s", state, execTime,
					c.c.RemoteAddr(), v)
			}
			i++
		}
		wg.Done()
	}

	// 并发执行n个请求
	offsert := 0
	for nodeName, co := range conns {
		s := sqls[nodeName] //[]string
		go f(rs, offsert, s, co)
		offsert += len(s)
	}

	wg.Wait()

	// 合并查询结果
	// 一个出错，全部出错
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

func (c *ClientConn) handleExec(stmt sqlparser.Statement, args []interface{}) error {
	plan, err := c.schema.rule.BuildPlan(c.db, stmt)
	if err != nil {
		return err
	}
	conns, err := c.getShardConns(false, plan)
	defer c.closeShardConns(conns, err != nil)
	if err != nil {
		log.ErrorErrorf(err, "ClientConn handleExec: %s", c.connectionId)
		return err
	}
	if conns == nil {
		return c.writeOK(nil)
	}

	var rs []*mysql.Result

	rs, err = c.executeInMultiNodes(conns, plan.RewrittenSqls, args)
	if err == nil {
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

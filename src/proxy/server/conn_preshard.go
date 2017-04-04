package server

import (
	"fmt"
	"strings"

	"kingshard/backend"
	"kingshard/core/errors"
	"kingshard/core/hack"
	"kingshard/mysql"
	"kingshard/proxy/router"
	"kingshard/sqlparser"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

type ExecuteDB struct {
	ExecNode *backend.Node
	IsSlave  bool
	sql      string
}

func (c *ClientConn) isBlacklistSql(sql string) bool {
	// fingerprint如何处理呢?
	fingerprint := mysql.GetFingerprint(sql)

	md5 := mysql.GetMd5(fingerprint)

	// 通过sql的md5来处理
	if _, ok := c.proxy.blacklistSqls[c.proxy.blacklistSqlsIndex].sqls[md5]; ok {
		return true
	}
	return false
}

//
//预处理sql
//
func (c *ClientConn) preHandleShard(sql string) (bool, error) {
	var rs []*mysql.Result
	var err error
	var executeDB *ExecuteDB

	if len(sql) == 0 {
		return false, errors.ErrCmdUnsupport
	}

	// 1. 如何禁止某个SQL语句呢?
	if c.proxy.blacklistSqls[c.proxy.blacklistSqlsIndex].sqlsLen != 0 {
		// 如果Blacklist，则返回
		if c.isBlacklistSql(sql) {
			log.Infof("Forbidden: %s:%s", c.c.RemoteAddr(), sql)
			err := mysql.NewError(mysql.ER_UNKNOWN_ERROR, "sql in blacklist.")
			return false, err
		}
	}

	// 2. 将sql分解成为tokens
	tokens := strings.FieldsFunc(sql, hack.IsSqlSep)

	if len(tokens) == 0 {
		return false, errors.ErrCmdUnsupport
	}

	// 3. 获取要执行的DB
	if c.isInTransaction() {
		executeDB, err = c.GetTransExecDB(tokens, sql)
	} else {
		executeDB, err = c.GetExecDB(tokens, sql)
	}

	if err != nil {
		// 如果出现错误，则直接返回给Client, 不用继续到backend去执行SQL
		if err == errors.ErrIgnoreSQL {
			err = c.writeOK(nil)
			if err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}

	// need shard sql
	if executeDB == nil {
		return false, nil
	}

	//作为Proxy需要将请求转发给后端!!!!
	//get connection in DB
	conn, err := c.getBackendConn(executeDB.ExecNode, executeDB.IsSlave)
	defer c.closeConn(conn, false)

	if err != nil {
		return false, err
	}
	// execute.sql may be rewritten in getShowExecDB
	rs, err = c.executeInNode(conn, executeDB.sql, nil)
	if err != nil {
		return false, err
	}

	if len(rs) == 0 {
		msg := fmt.Sprintf("result is empty")
		log.Errorf("ClientConn handleUnsupport: %s, sql: %s", msg, sql)
		return false, mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	c.lastInsertId = int64(rs[0].InsertId)
	c.affectedRows = int64(rs[0].AffectedRows)

	if rs[0].Resultset != nil {
		err = c.writeResultset(c.status, rs[0].Resultset)
	} else {
		err = c.writeOK(rs[0])
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *ClientConn) GetTransExecDB(tokens []string, sql string) (*ExecuteDB, error) {
	var err error
	tokensLen := len(tokens)

	// 获取执行的DB
	executeDB := new(ExecuteDB)
	executeDB.sql = sql

	//transaction execute in master db
	executeDB.IsSlave = false

	if 2 <= tokensLen {
		// /*node2*/ --> *node2*
		if tokens[0][0] == mysql.COMMENT_PREFIX {
			nodeName := strings.Trim(tokens[0], mysql.COMMENT_STRING)

			// 指定了节点
			if c.schema.nodes[nodeName] != nil {
				executeDB.ExecNode = c.schema.nodes[nodeName]
			}
		}
	}

	// 如果没有指定node, 那么也就是都是标准的mysql, 例如: insert, select等等
	if executeDB.ExecNode == nil {
		executeDB, err = c.GetExecDB(tokens, sql)
		if err != nil {
			return nil, err
		}
		if executeDB == nil {
			return nil, nil
		}
		return executeDB, nil
	}

	// 不能跨表支持事务
	if len(c.txConns) == 1 && c.txConns[executeDB.ExecNode] == nil {
		return nil, errors.ErrTransInMulti
	}
	return executeDB, nil
}

// if sql need shard return nil,
// else return the unshard db
//
func (c *ClientConn) GetExecDB(tokens []string, sql string) (*ExecuteDB, error) {
	tokensLen := len(tokens)

	// 这里不考虑/*node*/这种情况
	if 0 < tokensLen {
		tokenId, ok := mysql.PARSE_TOKEN_MAP[strings.ToLower(tokens[0])]
		if ok == true {
			// 第一个token应该为各种动作
			// select, delete, insert, update, replace, set, show, truncate
			// 好像不支持create table, alter table等?
			//
			switch tokenId {
			case mysql.TK_ID_SELECT:
				return c.getSelectExecDB(sql, tokens, tokensLen)
			case mysql.TK_ID_DELETE:
				return c.getDeleteExecDB(sql, tokens, tokensLen)
			case mysql.TK_ID_INSERT, mysql.TK_ID_REPLACE:
				return c.getInsertOrReplaceExecDB(sql, tokens, tokensLen)
			case mysql.TK_ID_UPDATE:
				return c.getUpdateExecDB(sql, tokens, tokensLen)
			case mysql.TK_ID_SET:
				return c.getSetExecDB(sql, tokens, tokensLen)

			case mysql.TK_ID_SHOW:
				return c.getShowExecDB(sql, tokens, tokensLen)

			case mysql.TK_ID_TRUNCATE:
				return c.getTruncateExecDB(sql, tokens, tokensLen)
			default:
				return nil, nil
			}
		}
	}

	// 如果没有给定Tokens, 然后呢?
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}
	return executeDB, nil
}

func (c *ClientConn) setExecuteNode(tokens []string, tokensLen int, executeDB *ExecuteDB) error {
	if 2 <= tokensLen {
		//for /*node1*/
		if 1 < len(tokens) && tokens[0][0] == mysql.COMMENT_PREFIX {
			nodeName := strings.Trim(tokens[0], mysql.COMMENT_STRING)
			if c.schema.nodes[nodeName] != nil {
				executeDB.ExecNode = c.schema.nodes[nodeName]
			}
			//for /*node1*/ select
			if strings.ToLower(tokens[1]) == mysql.TK_STR_SELECT {
				executeDB.IsSlave = true
			}
		}
	}

	if executeDB.ExecNode == nil {
		defaultRule := c.schema.rule.DefaultRule
		if len(defaultRule.Nodes) == 0 {
			return errors.ErrNoDefaultNode
		}
		executeDB.ExecNode = c.proxy.GetNode(defaultRule.Nodes[0])
	}

	return nil
}

//
//获取Select语句的ExecDB
//
func (c *ClientConn) getSelectExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	executeDB.IsSlave = true

	schema := c.proxy.schema
	router := schema.rule
	rules := router.Rules

	if len(rules) != 0 {
		for i := 1; i < tokensLen; i++ {
			if strings.ToLower(tokens[i]) == mysql.TK_STR_FROM {
				if i+1 < tokensLen {
					DBName, tableName := sqlparser.GetDBTable(tokens[i+1])

					// 两种DBName的获取方式，如果SQL中explicitly指定database, 那么以SQL为准；
					// 否则使用当前connection默认的DB
					// 一个DBConnection一次只能使用一个DB?
					// if the token[i+1] like this:kingshard.test_shard_hash
					if DBName != "" {
						ruleDB = DBName
					} else {
						ruleDB = c.db
					}

					// 如果DB/Table不存在Rule, 则在直接返回
					if router.GetRule(ruleDB, tableName) != router.DefaultRule {
						return nil, nil
					} else {
						//if the table is not shard table,send the sql
						//to default db
						break
					}
				}
			}

			// select last_insert_id(); 这个如何执行呢? 确实是一个难点
			if strings.ToLower(tokens[i]) == mysql.TK_STR_LAST_INSERT_ID {
				return nil, nil
			}
		}
	}

	//if send to master
	if 2 < tokensLen {
		if strings.ToLower(tokens[1]) == mysql.TK_STR_MASTER_HINT {
			executeDB.IsSlave = false
		}
	}
	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}
	return executeDB, nil
}

//get the execute database for delete sql
func (c *ClientConn) getDeleteExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	schema := c.proxy.schema
	router := schema.rule
	rules := router.Rules

	if len(rules) != 0 {
		for i := 1; i < tokensLen; i++ {
			if strings.ToLower(tokens[i]) == mysql.TK_STR_FROM {
				if i+1 < tokensLen {
					DBName, tableName := sqlparser.GetDBTable(tokens[i+1])
					//if the token[i+1] like this:kingshard.test_shard_hash
					if DBName != "" {
						ruleDB = DBName
					} else {
						ruleDB = c.db
					}
					if router.GetRule(ruleDB, tableName) != router.DefaultRule {
						return nil, nil
					} else {
						break
					}
				}
			}
		}
	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//get the execute database for insert or replace sql
func (c *ClientConn) getInsertOrReplaceExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	schema := c.proxy.schema
	router := schema.rule
	rules := router.Rules

	if len(rules) != 0 {
		for i := 0; i < tokensLen; i++ {
			if strings.ToLower(tokens[i]) == mysql.TK_STR_INTO {
				if i+1 < tokensLen {
					DBName, tableName := sqlparser.GetInsertDBTable(tokens[i+1])
					//if the token[i+1] like this:kingshard.test_shard_hash
					if DBName != "" {
						ruleDB = DBName
					} else {
						ruleDB = c.db
					}
					if router.GetRule(ruleDB, tableName) != router.DefaultRule {
						return nil, nil
					} else {
						break
					}
				}
			}
		}
	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//get the execute database for update sql
func (c *ClientConn) getUpdateExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	schema := c.proxy.schema
	router := schema.rule
	rules := router.Rules

	if len(rules) != 0 {
		for i := 0; i < tokensLen; i++ {
			if strings.ToLower(tokens[i]) == mysql.TK_STR_SET {
				DBName, tableName := sqlparser.GetDBTable(tokens[i-1])
				//if the token[i+1] like this:kingshard.test_shard_hash
				if DBName != "" {
					ruleDB = DBName
				} else {
					ruleDB = c.db
				}
				if router.GetRule(ruleDB, tableName) != router.DefaultRule {
					return nil, nil
				} else {
					break
				}
			}
		}
	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//get the execute database for set sql
func (c *ClientConn) getSetExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	executeDB := new(ExecuteDB)
	executeDB.sql = sql

	//handle three styles:
	//set autocommit= 0
	//set autocommit = 0
	//set autocommit=0
	if 2 <= len(tokens) {
		before := strings.Split(sql, "=")
		//uncleanWorld is 'autocommit' or 'autocommit '
		uncleanWord := strings.Split(before[0], " ")
		secondWord := strings.ToLower(uncleanWord[1])
		if _, ok := mysql.SET_KEY_WORDS[secondWord]; ok {
			return nil, nil
		}

		//SET [gobal/session] TRANSACTION ISOLATION LEVEL SERIALIZABLE
		//ignore this sql
		if 3 <= len(uncleanWord) {
			if strings.ToLower(uncleanWord[1]) == mysql.TK_STR_TRANSACTION ||
				strings.ToLower(uncleanWord[2]) == mysql.TK_STR_TRANSACTION {
				return nil, errors.ErrIgnoreSQL
			}
		}
	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//get the execute database for show sql
//choose slave preferentially
//tokens[0] is show
func (c *ClientConn) getShowExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	executeDB := new(ExecuteDB)
	executeDB.IsSlave = true
	executeDB.sql = sql

	//handle show columns/fields
	err := c.handleShowColumns(sql, tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	err = c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//handle show columns/fields
func (c *ClientConn) handleShowColumns(sql string, tokens []string,
	tokensLen int, executeDB *ExecuteDB) error {
	var ruleDB string
	for i := 0; i < tokensLen; i++ {
		tokens[i] = strings.ToLower(tokens[i])
		//handle SQL:
		//SHOW [FULL] COLUMNS FROM tbl_name [FROM db_name] [like_or_where]
		if (strings.ToLower(tokens[i]) == mysql.TK_STR_FIELDS ||
			strings.ToLower(tokens[i]) == mysql.TK_STR_COLUMNS) &&
			i+2 < tokensLen {
			if strings.ToLower(tokens[i+1]) == mysql.TK_STR_FROM {
				tableName := strings.Trim(tokens[i+2], "`")
				//get the ruleDB
				if i+4 < tokensLen && strings.ToLower(tokens[i+1]) == mysql.TK_STR_FROM {
					ruleDB = strings.Trim(tokens[i+4], "`")
				} else {
					ruleDB = c.db
				}
				showRouter := c.schema.rule
				showRule := showRouter.GetRule(ruleDB, tableName)
				//this SHOW is sharding SQL
				if showRule.Type != router.DefaultRuleType {
					if 0 < len(showRule.SubTableIndexs) {
						tableIndex := showRule.SubTableIndexs[0]
						nodeIndex := showRule.TableToNode[tableIndex]
						nodeName := showRule.Nodes[nodeIndex]
						tokens[i+2] = fmt.Sprintf("%s_%04d", tableName, tableIndex)
						executeDB.sql = strings.Join(tokens, " ")
						executeDB.ExecNode = c.schema.nodes[nodeName]
						return nil
					}
				}
			}
		}
	}
	return nil
}

//get the execute database for truncate sql
//sql: TRUNCATE [TABLE] tbl_name
func (c *ClientConn) getTruncateExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	schema := c.proxy.schema
	router := schema.rule
	rules := router.Rules
	if len(rules) != 0 && tokensLen >= 2 {
		DBName, tableName := sqlparser.GetDBTable(tokens[tokensLen-1])
		//if the token[i+1] like this:kingshard.test_shard_hash
		if DBName != "" {
			ruleDB = DBName
		} else {
			ruleDB = c.db
		}
		if router.GetRule(ruleDB, tableName) != router.DefaultRule {
			return nil, nil
		}

	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

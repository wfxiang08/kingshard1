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

	"mysql"

	"backend"
	"config"
	"core/errors"
	"proxy/router"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

type Schema struct {
	nodes map[string]*backend.Node
	rule  *router.Router
}

type BlacklistSqls struct {
	sqls    map[string]string
	sqlsLen int
}

// 定义节点的状态
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
	//db       string

	// 这些是什么逻辑?
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
	running  bool
}

// 返回Server的状态
func (s *Server) Status() string {
	var status string
	switch s.status[s.statusIndex] {
	case Online:
		status = "online"
	case Offline:
		status = "offline"
	default:
		status = "unknown"
	}
	return status
}

//TODO
func (s *Server) parseAllowIps() error {
	// 切换版本?
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

//
// Node是什么概念呢？
//
func (s *Server) parseNode(cfg config.NodeConfig) (*backend.Node, error) {
	var err error
	// 在配置文件中有Node的定义
	n := new(backend.Node)
	n.Cfg = cfg

	//name : node1
	//
	//# default max conns for mysql server
	//max_conns_limit : 32
	//
	//# all mysql in a node must have the same user and password
	//user :  root
	//password : root
	//
	//# master represents a real mysql master server
	//master : 127.0.0.1:3307
	//
	//# slave represents a real mysql salve server,and the number after '@' is
	//# read load weight of this slave.
	//#slave : 192.168.59.101:3307@2,192.168.59.101:3307@3
	//down_after_noalive : 32
	//
	n.DownAfterNoAlive = time.Duration(cfg.DownAfterNoAlive) * time.Second
	err = n.ParseMaster(cfg.Master)
	if err != nil {
		return nil, err
	}
	err = n.ParseSlave(cfg.Slave)
	if err != nil {
		return nil, err
	}

	go n.CheckNode()

	return n, nil
}

//
// 解析每一个Node的信息
// IP, Port, User/password
func (s *Server) parseNodes() error {
	cfg := s.cfg

	// 要求节点定义无重复
	s.nodes = make(map[string]*backend.Node, len(cfg.Nodes))
	for _, v := range cfg.Nodes {
		if _, ok := s.nodes[v.Name]; ok {
			return fmt.Errorf("duplicate node [%s]", v.Name)
		}

		n, err := s.parseNode(v)
		if err != nil {
			return err
		}

		s.nodes[v.Name] = n
	}

	return nil
}

func (s *Server) parseSchema() error {
	schemaCfg := s.cfg.Schema
	if len(schemaCfg.Nodes) == 0 {
		return fmt.Errorf("schema must have a node")
	}

	// 解析数据库节点
	nodes := make(map[string]*backend.Node)
	for _, n := range schemaCfg.Nodes {
		if s.GetNode(n) == nil {
			return fmt.Errorf("schema node [%s] config is not exists", n)
		}

		if _, ok := nodes[n]; ok {
			return fmt.Errorf("schema node [%s] duplicate", n)
		}

		nodes[n] = s.GetNode(n)
	}

	// 解析rule
	rule, err := router.NewRouter(&schemaCfg)
	if err != nil {
		return err
	}

	// 节点 + 规则 ==> 完整的数据库
	s.schema = &Schema{
		nodes: nodes,
		rule:  rule,
	}

	return nil
}

func NewServer(cfg *config.Config) (*Server, error) {
	s := new(Server)

	s.cfg = cfg
	s.counter = new(Counter)
	s.addr = cfg.Addr
	s.user = cfg.User
	s.password = cfg.Password

	// 初始状态
	atomic.StoreInt32(&s.statusIndex, 0)
	s.status[s.statusIndex] = Online
	atomic.StoreInt32(&s.logSqlIndex, 0)
	s.logSql[s.logSqlIndex] = cfg.LogSql
	atomic.StoreInt32(&s.slowLogTimeIndex, 0)
	s.slowLogTime[s.slowLogTimeIndex] = cfg.SlowLogTime

	// XXX: 字符集的设计
	if len(cfg.Charset) == 0 {
		cfg.Charset = mysql.DEFAULT_CHARSET //utf8
	}
	cid, ok := mysql.CharsetIds[cfg.Charset]
	if !ok {
		return nil, errors.ErrInvalidCharset
	}

	// XXX: Default的参数也是可以修改的
	//change the default charset
	mysql.DEFAULT_CHARSET = cfg.Charset
	mysql.DEFAULT_COLLATION_ID = cid
	mysql.DEFAULT_COLLATION_NAME = mysql.Collations[cid]

	if err := s.parseBlackListSqls(); err != nil {
		return nil, err
	}

	if err := s.parseAllowIps(); err != nil {
		return nil, err
	}

	if err := s.parseNodes(); err != nil {
		return nil, err
	}

	if err := s.parseSchema(); err != nil {
		return nil, err
	}

	// 监听tcp socket
	// TODO: 支持unix domain socket, 参考rpc_proxy来写
	var err error
	netProto := "tcp"
	s.listener, err = net.Listen(netProto, s.addr)

	if err != nil {
		return nil, err
	}

	log.Printf("Proxy server running: %s", s.addr)

	return s, nil
}

func (s *Server) flushCounter() {
	for {
		s.counter.FlushCounter()
		time.Sleep(1 * time.Second)
	}
}

//
// 接受来自业务端的mysql的连接: ClienConn
//
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

	c.schema = s.GetSchema()

	c.pkg = mysql.NewPacketIO(tcpConn)
	c.proxy = s

	c.pkg.Sequence = 0

	c.connectionId = atomic.AddUint32(&baseConnId, 1)

	c.status = mysql.SERVER_STATUS_AUTOCOMMIT

	c.salt, _ = mysql.RandomBuf(20)

	c.txConns = make(map[*backend.Node]*backend.BackendConn)

	c.closed = false

	c.charset = mysql.DEFAULT_CHARSET
	c.collation = mysql.DEFAULT_COLLATION_ID

	c.stmtId = 0
	c.stmts = make(map[uint32]*Stmt)

	return c
}

func (s *Server) onConn(c net.Conn) {
	// 计数器
	s.counter.IncrClientConns()

	// net.Conn 封装 ClientConn
	// 底层的[]byte协议 --> Io Packet协议
	//
	conn := s.newClientConn(c) //新建一个conn

	defer func() {
		// 处理未处理的异常
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)

			// 获取runtime.Stack
			buf = buf[:runtime.Stack(buf, false)] //获得当前goroutine的stacktrace
			log.Errorf("server onConn, remoteAddr: %s, err: %v, statck: %s", c.RemoteAddr().String(), err, string(buf))

		}

		// 关闭Connection，处理计数器
		// conn 多次Close不会有副作用
		conn.Close()
		s.counter.DecrClientConns()
	}()

	// 检查权限
	if allowConnect := conn.IsAllowConnect(); allowConnect == false {
		err := mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR, "ip address access denied by kingshard.")
		conn.writeError(err)
		conn.Close()
		return
	}
	if err := conn.Handshake(); err != nil {
		log.ErrorErrorf(err, "Handshake error")
		conn.writeError(err)
		conn.Close()
		return
	}

	conn.Run()
}

func (s *Server) ChangeProxy(v string) error {
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

func (s *Server) ChangeLogSql(v string) error {
	v = strings.ToLower(v)
	//if v != golog.LogSqlOn && v != golog.LogSqlOff {
	//	return errors.ErrCmdUnsupport
	//}
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

func (s *Server) ChangeSlowLogTime(v string) error {
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

func (s *Server) AddAllowIP(v string) error {
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

func (s *Server) DelAllowIP(v string) error {
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

func (s *Server) GetAllBlackSqls() []string {
	blackSQLs := make([]string, 0, 10)
	for _, SQL := range s.blacklistSqls[s.blacklistSqlsIndex].sqls {
		blackSQLs = append(blackSQLs, SQL)
	}
	return blackSQLs
}

func (s *Server) AddBlackSql(v string) error {
	v = strings.TrimSpace(v)
	fingerPrint := mysql.GetFingerprint(v)
	md5 := mysql.GetMd5(fingerPrint)
	if s.blacklistSqlsIndex == 0 {
		if _, ok := s.blacklistSqls[0].sqls[md5]; ok {
			return errors.ErrBlackSqlExist
		}
		s.blacklistSqls[1] = s.blacklistSqls[0]
		s.blacklistSqls[1].sqls[md5] = v
		s.blacklistSqls[1].sqlsLen += 1
		atomic.StoreInt32(&s.blacklistSqlsIndex, 1)
	} else {
		if _, ok := s.blacklistSqls[1].sqls[md5]; ok {
			return errors.ErrBlackSqlExist
		}
		s.blacklistSqls[0] = s.blacklistSqls[1]
		s.blacklistSqls[0].sqls[md5] = v
		s.blacklistSqls[0].sqlsLen += 1
		atomic.StoreInt32(&s.blacklistSqlsIndex, 0)
	}

	return nil
}

func (s *Server) DelBlackSql(v string) error {
	v = strings.TrimSpace(v)
	fingerPrint := mysql.GetFingerprint(v)
	md5 := mysql.GetMd5(fingerPrint)

	if s.blacklistSqlsIndex == 0 {
		if _, ok := s.blacklistSqls[0].sqls[md5]; !ok {
			return errors.ErrBlackSqlNotExist
		}
		s.blacklistSqls[1] = s.blacklistSqls[0]
		s.blacklistSqls[1].sqls[md5] = v
		delete(s.blacklistSqls[1].sqls, md5)
		s.blacklistSqls[1].sqlsLen -= 1
		atomic.StoreInt32(&s.blacklistSqlsIndex, 1)
	} else {
		if _, ok := s.blacklistSqls[1].sqls[md5]; !ok {
			return errors.ErrBlackSqlNotExist
		}
		s.blacklistSqls[0] = s.blacklistSqls[1]
		s.blacklistSqls[0].sqls[md5] = v
		delete(s.blacklistSqls[0].sqls, md5)
		s.blacklistSqls[0].sqlsLen -= 1
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
		log.ErrorErrorf(err, "saveBlackSql create file error, blacklist_sql_file: %s", s.cfg.BlsFile)
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

func (s *Server) SaveProxyConfig() error {
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

//
// 监听端口，Running
//
func (s *Server) Run() error {
	log.Info("Proxy server run...")
	s.running = true

	// flush counter
	go s.flushCounter()

	// 似乎不用考虑同步的问题
	for s.running {
		// 监听socket
		// s.listener.Close() 或者有新请求，则返回
		conn, err := s.listener.Accept()
		log.Info("Proxy get new connection")
		if err != nil {
			log.ErrorErrorf(err, "server Run")
			continue
		}

		// 处理单个的Connection
		go s.onConn(conn)
	}

	return nil
}

//
// 结束Running的状态，并且关闭端口监听
//
func (s *Server) Close() {
	s.running = false

	// 关闭端口监听
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) DeleteSlave(node string, addr string) error {
	addr = strings.Split(addr, backend.WeightSplit)[0]
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	if err := n.DeleteSlave(addr); err != nil {
		return err
	}

	//sync node slave to global config
	for i, v1 := range s.cfg.Nodes {
		if node == v1.Name {
			s1 := strings.Split(v1.Slave, backend.SlaveSplit)
			s2 := make([]string, 0, len(s1)-1)
			for _, v2 := range s1 {
				hostPort := strings.Split(v2, backend.WeightSplit)[0]
				if addr != hostPort {
					s2 = append(s2, v2)
				}
			}
			s.cfg.Nodes[i].Slave = strings.Join(s2, backend.SlaveSplit)
		}
	}

	return nil
}

func (s *Server) AddSlave(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	if err := n.AddSlave(addr); err != nil {
		return err
	}

	//sync node slave to global config
	for i, v1 := range s.cfg.Nodes {
		if v1.Name == node {
			s1 := strings.Split(v1.Slave, backend.SlaveSplit)
			s1 = append(s1, addr)
			s.cfg.Nodes[i].Slave = strings.Join(s1, backend.SlaveSplit)
		}
	}

	return nil
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

// 手动下线
func (s *Server) DownMaster(node, masterAddr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}
	return n.DownMaster(masterAddr, backend.ManualDown)
}

// 手动下线
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

func (s *Server) GetAllNodes() map[string]*backend.Node {
	return s.nodes
}

func (s *Server) GetSchema() *Schema {
	return s.schema
}

func (s *Server) GetSlowLogTime() int {
	return s.slowLogTime[s.slowLogTimeIndex]
}

func (s *Server) GetAllowIps() []string {
	var ips []string
	for _, v := range s.allowips[s.allowipsIndex] {
		if v != nil {
			ips = append(ips, v.String())
		}
	}
	return ips
}

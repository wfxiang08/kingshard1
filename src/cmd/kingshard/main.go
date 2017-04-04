package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"config"
	"core/hack"
	"proxy/server"
	"web"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"strings"
)

var configFile *string = flag.String("config", "/etc/ks.yaml", "kingshard config file")
var logLevel *string = flag.String("log-level", "", "log level [debug|info|warn|error], default error")
var version *bool = flag.Bool("v", false, "the version of kingshard")

const (
	sqlLogName = "sql.log"
	sysLogName = "sys.log"
	MaxLogSize = 1024 * 1024 * 1024
)

const banner string = `kingshard proxy`

func main() {
	fmt.Print(banner)
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	fmt.Printf("Git commit:%s, Build time:%s\n", hack.Version, hack.Compile)

	if *version {
		return
	}
	if len(*configFile) == 0 {
		fmt.Println("must use a config file")
		return
	}

	// 解析配置文件
	cfg, err := config.ParseConfigFile(*configFile)
	if err != nil {
		fmt.Printf("parse config file error:%v\n", err.Error())
		return
	}

	log.SetLevel(log.LEVEL_INFO)
	maxKeepDays := 3
	// 设置Log文件
	//when the log file size greater than 1GB, kingshard will generate a new file
	if len(cfg.LogPath) != 0 {
		f, err := log.NewRollingFile(cfg.LogPath, maxKeepDays)
		if err != nil {
			log.PanicErrorf(err, "open rolling log file failed: %s", cfg.LogPath)
		} else {
			defer f.Close()
			log.StdLog = log.New(f, "")
		}
	}

	if *logLevel != "" {
		setLogLevel(*logLevel)
	} else {
		setLogLevel(cfg.LogLevel)
	}

	var svr *server.Server
	var apiSvr *web.ApiServer

	// 最核心的逻辑： Server
	svr, err = server.NewServer(cfg)

	if err != nil {
		log.ErrorErrorf(err, "server.NewServer failed")
		return
	}
	apiSvr, err = web.NewApiServer(cfg, svr)
	if err != nil {
		log.ErrorErrorf(err, "web.NewApiServer failed")
		svr.Close()
		return
	}

	// 监听signal
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGPIPE)

	go func() {
		for {
			// 异步处理信号
			sig := <-sc
			if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
				log.Printf("receive quit signal")

				// 关闭Server, 然后: srv.run就结束
				svr.Close()
			} else if sig == syscall.SIGPIPE {
				log.Printf("Ignore broken pipe signal")
			}
		}
	}()
	// 对外提供API服务
	go apiSvr.Run()

	// 提供mysql server的服务
	svr.Run()
}

func setLogLevel(level string) {
	var lv = log.LEVEL_INFO
	switch strings.ToLower(level) {
	case "error":
		lv = log.LEVEL_ERROR
	case "warn", "warning":
		lv = log.LEVEL_WARN
	case "debug":
		lv = log.LEVEL_DEBUG
	case "info":
		fallthrough
	default:
		lv = log.LEVEL_INFO
	}
	log.SetLevel(lv)
	log.Infof("set log level to %s", lv)
}

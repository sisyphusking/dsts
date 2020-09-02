package config

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type Config struct {
	Role        string
	NodeAddr    string
	Coordinator string
	Followers   []string
	Whitelist   []string
	CommitType  string
	Timeout     uint64
	//DBSchema    string
	Hooks string
}

type followers []string

func (f *followers) String() string {
	return strings.Join(*f, ",")
}

func (f *followers) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type whitelist []string

func (w *whitelist) String() string {
	return strings.Join(*w, ",")
}

func (w *whitelist) Set(value string) error {
	*w = append(*w, value)
	return nil
}

func Get() *Config {

	//配置文件初始化
	if err := Init(); err != nil {
		panic(err)
	}

	var (
		followersArr followers
		whitelistArr whitelist
	)
	cfg := flag.String("config", "", "path to config")
	role := flag.String("role", "follower", "role (coordinator of follower)")
	nodeaddr := flag.String("nodeaddr", "localhost:3050", "node address")
	coordinator := flag.String("coordinator", "", "coordinator address")
	commitType := flag.String("committype", "two-phase", "two-phase or three-phase commit mode")
	timeout := flag.Uint64("timeout", 1000, "ms, timeout after which the message is considered unacknowledged (only for three-phase mode, because two-phase is blocking by design)")
	hooks := flag.String("hooks", "hooks/src/hooks.go", "path to hooks file on filesystem")
	flag.Var(&followersArr, "follower", "follower address")
	flag.Var(&whitelistArr, "whitelist", "allowed hosts")
	flag.Parse()

	//没有指定配置文件
	if *cfg == "" {
		if *role != "coordinator" {
			if !Includes(followersArr, *nodeaddr) {
				followersArr = append(followersArr, *nodeaddr)
			}
		}
		if !Includes(whitelistArr, "127.0.0.1") {
			whitelistArr = append(whitelistArr, "127.0.0.1")
		}
		return &Config{*role, *nodeaddr, *coordinator,
			followersArr, whitelistArr, *commitType,
			*timeout, *hooks}
	}

	//指定了配置文件
	var svrConfig Config
	viper.SetConfigFile("server")
	viper.SetConfigType("yml")
	viper.AddConfigPath(*cfg)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(fmt.Sprintf("Error reading config file, %s", err))
	}
	err := viper.Unmarshal(&svrConfig)
	if err != nil {
		zap.L().Fatal("Unable to unmarshal config")
	}

	if svrConfig.Role != "coordinator" {
		if !Includes(svrConfig.Followers, svrConfig.NodeAddr) {
			svrConfig.Followers = append(svrConfig.Followers, svrConfig.NodeAddr)
		}
	}

	if !Includes(svrConfig.Whitelist, "127.0.0.1") {
		svrConfig.Whitelist = append(svrConfig.Whitelist, "127.0.0.1")
	}

	return &Config{svrConfig.Role, svrConfig.NodeAddr,
		svrConfig.Coordinator, svrConfig.Followers,
		svrConfig.Whitelist, svrConfig.CommitType,
		svrConfig.Timeout, svrConfig.Hooks}
}

func Includes(arr []string, value string) bool {
	for i := range arr {
		if arr[i] == value {
			return true
		}
	}
	return false
}

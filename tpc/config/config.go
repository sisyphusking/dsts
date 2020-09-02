package config

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/fsnotify/fsnotify"
	"github.com/natefinch/lumberjack"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type YamlConfig struct {
	Name string
}

func Init() error {
	c := YamlConfig{}

	// 初始化配置文件
	if err := c.initConfig(); err != nil {
		return err
	}

	// 初始化日志包
	//c.initLogger()
	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	// 监控配置文件变化并热加载程序
	c.watchConfig()

	return nil
}

func (c *YamlConfig) initConfig() error {
	if c.Name != "" {
		viper.SetConfigFile(c.Name) // 如果指定了配置文件，则解析指定的配置文件
	} else {
		viper.AddConfigPath("config") // 如果没有指定配置文件，则解析默认的配置文件
		viper.SetConfigName("config")
	}
	viper.SetConfigType("yaml")                  // 设置配置文件格式为YAML
	if err := viper.ReadInConfig(); err != nil { // viper解析配置文件
		return err
	}
	//合并yaml文件
	//viper.SetConfigName("service")
	//if mergeErr := viper.MergeInConfig(); mergeErr != nil {
	//	return mergeErr
	//}

	return nil
}

// 监控配置文件变化并热加载程序
func (c *YamlConfig) watchConfig() {
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		log.Info(fmt.Sprintf("Config file changed: %s", e.Name))
	})
}

func (c *YamlConfig) initLogger() *zap.Logger {

	hook := lumberjack.Logger{
		Filename:   viper.GetString("log.logger_file"),  // 日志文件路径
		MaxSize:    viper.GetInt("log.log_max_size"),    // megabytes
		MaxBackups: viper.GetInt("log.log_max_backups"), // 最多保留3个备份
		MaxAge:     viper.GetInt("log.log_max_age"),     //days
		Compress:   true,                                // 是否压缩 disabled by default
	}
	w := zapcore.AddSync(&hook)
	logLevel := strings.ToLower(viper.GetString("log.logger_level"))
	var level zapcore.Level
	switch logLevel {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "error":
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		w,
		level,
	)

	logger := zap.New(core)
	zap.ReplaceGlobals(logger)

	return logger
}

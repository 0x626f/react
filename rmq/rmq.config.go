package rmq

import (
	"net"
	neturl "net/url"
	"strconv"
	"time"

	"github.com/0x626f/gioc"
)

const ModuleConfigToken = gioc.Token("RmqModuleConfig")

func ProvideModuleConfig(config *ModuleConfig) gioc.IProvider {
	return gioc.ValueProvider(ModuleConfigToken, config, false)
}

type ModuleConfig struct {
	Host        string `env:"RMQ_HOST"`
	Port        int    `env:"RMQ_PORT"`
	User        string `env:"RMQ_USER"`
	Password    string `env:"RMQ_PASSWORD"`
	VirtualHost string `env:"RMQ_VIRTUAL_HOST"`

	RetryCount int           `env:"RMQ_CONNECTION_RETRY_COUNT"`
	RetryDelay time.Duration `env:"RMQ_CONNECTION_RETRY_DELAY"`
}

func (config *ModuleConfig) buildConnectionUrl() string {
	connection := &neturl.URL{
		Scheme: "amqp",
		Host:   net.JoinHostPort(config.Host, strconv.Itoa(config.Port)),
	}

	if config.User != "" && config.Password != "" {
		connection.User = neturl.UserPassword(config.User, config.Password)
	}
	if config.VirtualHost != "" {
		connection.Path = "/" + config.VirtualHost
		connection.RawPath = "/" + neturl.PathEscape(config.VirtualHost)
	}

	return connection.String()
}

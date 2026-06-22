package redis

import (
	"errors"
	"fmt"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
	goredis "github.com/redis/go-redis/v9"
)

var ServiceInjections = react.InjectFromBase(ConfigToken)

type Service struct {
	react.BaseConfigurableService[*Config]

	client *goredis.Client
}

func NewService(injections gioc.Injections) (*Service, error) {
	gioc.Require(injections, ServiceInjections...)

	service := &Service{}
	service.Bootstrap(ServiceToken, ConfigToken, injections)
	if service.Config == nil || service.Config.URL == "" {
		return nil, fmt.Errorf("redis url is required")
	}

	options, err := goredis.ParseURL(service.Config.URL)
	if err != nil {
		return nil, err
	}

	service.client = goredis.NewClient(options)
	if err = service.client.Ping(service.Ctx).Err(); err != nil {
		_ = service.client.Close()
		return nil, err
	}

	service.ApplicationService.AddHook(func() {
		_ = service.client.Close()
	})

	return service, nil
}

func (service *Service) Set(key string, value any, ttl time.Duration) error {
	return service.client.Set(service.Ctx, key, value, ttl).Err()
}

func (service *Service) SetIfNotExists(key string, value any, ttl time.Duration) (bool, error) {
	return service.client.SetNX(service.Ctx, key, value, ttl).Result()
}

func (service *Service) Get(key string) (string, bool, error) {
	value, err := service.client.Get(service.Ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	return value, true, nil
}

func (service *Service) GetBool(key string) (bool, bool, error) {
	value, err := service.client.Get(service.Ctx, key).Bool()
	if errors.Is(err, goredis.Nil) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return value, true, nil
}

func (service *Service) Keys(template string) ([]string, error) {
	return service.client.Keys(service.Ctx, template).Result()
}

func (service *Service) Scan(template string, count int64) ([]string, error) {
	var (
		cursor uint64
		keys   []string
	)

	for {
		batch, next, err := service.client.Scan(service.Ctx, cursor, template, count).Result()
		if err != nil {
			return nil, err
		}

		keys = append(keys, batch...)
		if next == 0 {
			break
		}
		cursor = next
	}

	return keys, nil
}

func (service *Service) Add(key string, members ...any) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	return service.client.SAdd(service.Ctx, key, members...).Result()
}

func (service *Service) Size(key string) (int64, error) {
	return service.client.SCard(service.Ctx, key).Result()
}

func (service *Service) Drop(key string, members ...any) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	return service.client.SRem(service.Ctx, key, members...).Result()
}

func (service *Service) IncludesMany(key string, members ...string) ([]bool, error) {
	return service.client.SMIsMember(service.Ctx, key, members).Result()
}

func (service *Service) IncludesOne(key string, member any) (bool, error) {
	return service.client.SIsMember(service.Ctx, key, member).Result()
}

func (service *Service) Delete(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	return service.client.Del(service.Ctx, keys...).Err()
}

package postgres

type Config struct {
	URL string `env:"POSTGRES_URL"`
}

package clickhouse

import (
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/XSAM/otelsql"
	"github.com/jmoiron/sqlx"
)

type Client struct {
	*sqlx.DB
}

type ClientConfig struct {
	Servers  []string
	Username string
	Password string
	Options  []string
}

func New(ch *ClientConfig) (*Client, error) {
	if len(ch.Servers) == 0 {
		return nil, fmt.Errorf("no servers provided")
	}
	options := []string{
		fmt.Sprintf("username=%s", ch.Username),
		fmt.Sprintf("password=%s", ch.Password),
	}
	options = append(options, ch.Options...)
	allOptions := strings.Join(options, "&")
	driver, err := otelsql.Register("clickhouse")
	if err != nil {
		return nil, fmt.Errorf("registering ClickHouse driver for instrumentation: %v", err)
	}
	db, err := sqlx.Connect(driver, fmt.Sprintf("clickhouse://%s?%s", ch.Servers[0], allOptions))
	//db, err := sqlx.Connect("clickhouse", chUri)

	if err != nil {
		return nil, err
	}
	return &Client{db}, nil
}

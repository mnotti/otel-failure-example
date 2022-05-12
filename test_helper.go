package clickhouse

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	imageM1                   = "altinity/clickhouse-server:21.12.3.32.altinitydev.arm"
	imageIntel                = "altinity/clickhouse-server:21.8.10.1.altinitystable"
	port                      = "9000"
	healthCheckRetries        = 10
	healthCheckInitialBackoff = 1 * time.Second
	healthCheckMaxBackoff     = 10 * time.Second
)

type TestContainer struct {
	testcontainers.Container
	Addr string
}

func CreateClickHouseContainer(ctx context.Context, t *testing.T) (*TestContainer, func()) {
	image := imageIntel
	if runtime.GOARCH == "arm64" {
		image = imageM1
	}
	chCtr, err := EnsureContainer(ctx, image, []string{port}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return chCtr, func() { chCtr.Terminate(ctx) }
}

func CreateClickHouseClientAndServer(ctx context.Context, t *testing.T) (*Client, func()) {
	chCtr, cleanup := CreateClickHouseContainer(ctx, t)
	ch, err := New(&ClientConfig{Servers: []string{chCtr.Addr}})
	if err != nil {
		t.Fatal(err)
	}
	if err := WaitFor(ctx, healthCheckInitialBackoff, healthCheckMaxBackoff, healthCheckRetries,
		func(ctx context.Context) error {
			return ch.PingContext(ctx)
		},
	); err != nil {
		t.Fatal("ClickHouse server unhealthy: " + err.Error())
	}
	return ch, func() {
		ch.Close()
		cleanup()
	}
}

func EnsureContainer(ctx context.Context, image string, ports []string, env map[string]string, cmd []string) (*TestContainer, error) {
	exposedPorts := make([]string, len(ports))
	for i := range ports {
		exposedPorts[i] = fmt.Sprintf("%s/tcp", ports[i])
	}
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: exposedPorts,
		WaitingFor:   wait.ForListeningPort(nat.Port(ports[0])),
		Env:          env,
	}
	if len(cmd) > 0 {
		req.Cmd = cmd
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	ip, err := ctr.Host(ctx)
	if err != nil {
		return nil, err
	}
	mappedPort, err := ctr.MappedPort(ctx, nat.Port(ports[0]))
	if err != nil {
		return nil, err
	}
	return &TestContainer{Container: ctr, Addr: fmt.Sprintf("%s:%s", ip, mappedPort.Port())}, nil
}

func WaitFor(ctx context.Context, initialBackoff, maxBackoff time.Duration, retries int, check func(ctx context.Context) error) error {
	backoff := initialBackoff
	var err error
	for i := 0; i < retries; i++ {
		ctxTimeout, cancel := context.WithTimeout(ctx, backoff)
		defer cancel()
		if err = check(ctxTimeout); err == nil {
			return nil
		}
		backoff = backoff * 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return err
}

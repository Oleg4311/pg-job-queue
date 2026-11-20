// Пакет pgtest поднимает PostgreSQL для тестов: берёт DATABASE_URL (CI, docker compose),
// а если переменной нет — запускает встроенный Postgres (скачивает бинарники при первом запуске).
package pgtest

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	embedded "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Start возвращает пул и функцию остановки.
func Start(ctx context.Context) (*pgxpool.Pool, func(), error) {
	url := os.Getenv("DATABASE_URL")
	stop := func() {}
	if url == "" {
		port, err := freePort()
		if err != nil {
			return nil, nil, err
		}
		base := filepath.Join(os.TempDir(), "pgjq-embedded")
		pg := embedded.NewDatabase(embedded.DefaultConfig().
			Version(embedded.V16). // версия закреплена: «последняя» бывает ещё недоступна для некоторых платформ
			Port(uint32(port)).
			RuntimePath(filepath.Join(base, fmt.Sprint(port))).
			CachePath(filepath.Join(base, "cache")).
			Logger(nil))
		if err := pg.Start(); err != nil {
			return nil, nil, fmt.Errorf("встроенный postgres: %w", err)
		}
		stop = func() { _ = pg.Stop() }
		url = fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		stop()
		return nil, nil, err
	}
	cfg.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		stop()
		return nil, nil, err
	}
	return pool, func() { pool.Close(); stop() }, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

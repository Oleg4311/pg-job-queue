// devdb запускает одноразовый встроенный PostgreSQL и печатает строку подключения.
// Нужен, чтобы попробовать pgjq без Docker: go run ./internal/devdb
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/Oleg4311/pg-job-queue/internal/pgtest"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Unsetenv("DATABASE_URL")
	pool, closeDB, err := pgtest.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer closeDB()
	cfg := pool.Config().ConnConfig
	fmt.Printf("DATABASE_URL=postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable\n", cfg.Port)
	<-ctx.Done()
}

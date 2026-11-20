// pgjq — командная строка для очереди jobq: миграции, постановка задач, статистика, демо-воркер и бенчмарк.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Oleg4311/pg-job-queue/jobq"
	"github.com/jackc/pgx/v5/pgxpool"
)

var version = "0.1.0"

const usage = `pgjq — очередь задач на PostgreSQL

Использование: pgjq <команда> [флаги]
Общие флаги: --dsn (или переменная DATABASE_URL), --queue (по умолчанию default)

Команды:
  migrate                         создать/обновить схему
  enqueue --kind K [--payload J] [--priority N] [--delay 5s] [--max-attempts N] [--dedupe KEY]
  stats [--watch 1s]              сводка по очереди
  work [--concurrency N] [--lease 30s]
                                  демо-воркер с обработчиками echo, sleep, fail, flaky
  dead [--limit N]                список задач в dead
  requeue --id N                  вернуть dead-задачу в очередь
  bench [--jobs N] [--workers W] [--concurrency C]
                                  измерить пропускную способность на вашем Postgres
  version
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "version", "--version":
		fmt.Println("pgjq", version)
		return
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, cmd, args); err != nil {
		fmt.Fprintln(os.Stderr, "ошибка:", err)
		os.Exit(1)
	}
}

type common struct {
	dsn   string
	queue string
}

func newFlags(name string) (*flag.FlagSet, *common) {
	c := &common{}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.StringVar(&c.dsn, "dsn", os.Getenv("DATABASE_URL"), "строка подключения к PostgreSQL")
	fs.StringVar(&c.queue, "queue", "default", "имя очереди")
	return fs, c
}

func connect(ctx context.Context, c *common, conns int32) (*pgxpool.Pool, error) {
	if c.dsn == "" {
		return nil, errors.New("не задан --dsn или DATABASE_URL")
	}
	cfg, err := pgxpool.ParseConfig(c.dsn)
	if err != nil {
		return nil, err
	}
	if conns > 0 {
		cfg.MaxConns = conns
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

func run(ctx context.Context, cmd string, args []string) error {
	fs, c := newFlags(cmd)
	switch cmd {
	case "migrate":
		if err := fs.Parse(args); err != nil {
			return err
		}
		pool, err := connect(ctx, c, 0)
		if err != nil {
			return err
		}
		defer pool.Close()
		if err := jobq.Migrate(ctx, pool); err != nil {
			return err
		}
		fmt.Println("схема актуальна")
	case "enqueue":
		kind := fs.String("kind", "", "тип задачи (обязательно)")
		payload := fs.String("payload", "{}", "payload в JSON")
		prio := fs.Int("priority", 0, "приоритет (больше — раньше)")
		delay := fs.Duration("delay", 0, "отложить выполнение")
		maxAtt := fs.Int("max-attempts", 5, "максимум попыток")
		dedupe := fs.String("dedupe", "", "ключ дедупликации")
		if err := fs.Parse(args); err != nil {
			return err
		}
		var raw json.RawMessage
		if err := json.Unmarshal([]byte(*payload), &raw); err != nil {
			return fmt.Errorf("--payload не JSON: %w", err)
		}
		pool, err := connect(ctx, c, 0)
		if err != nil {
			return err
		}
		defer pool.Close()
		opts := []jobq.EnqueueOption{jobq.Queue(c.queue), jobq.Priority(*prio), jobq.Delay(*delay), jobq.MaxAttempts(*maxAtt)}
		if *dedupe != "" {
			opts = append(opts, jobq.DedupeKey(*dedupe))
		}
		id, inserted, err := jobq.Enqueue(ctx, pool, *kind, raw, opts...)
		if err != nil {
			return err
		}
		if inserted {
			fmt.Printf("задача %d поставлена\n", id)
		} else {
			fmt.Printf("дубль: уже есть живая задача %d\n", id)
		}
	case "stats":
		watch := fs.Duration("watch", 0, "обновлять раз в N (0 — один раз)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		pool, err := connect(ctx, c, 0)
		if err != nil {
			return err
		}
		defer pool.Close()
		for {
			s, err := jobq.GetStats(ctx, pool, c.queue)
			if err != nil {
				return err
			}
			fmt.Printf("очередь %-10s queued=%d running=%d done=%d dead=%d  старейшая ждёт %v\n",
				c.queue, s.Queued, s.Running, s.Done, s.Dead, s.OldestQueuedAge.Round(time.Millisecond))
			if *watch <= 0 {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(*watch):
			}
		}
	case "work":
		return work(ctx, fs, c, args)
	case "dead":
		limit := fs.Int("limit", 20, "сколько показать")
		if err := fs.Parse(args); err != nil {
			return err
		}
		pool, err := connect(ctx, c, 0)
		if err != nil {
			return err
		}
		defer pool.Close()
		dead, err := jobq.ListDead(ctx, pool, c.queue, *limit)
		if err != nil {
			return err
		}
		if len(dead) == 0 {
			fmt.Println("кладбище пусто")
		}
		for _, d := range dead {
			fmt.Printf("#%d %s попыток=%d: %s\n", d.ID, d.Kind, d.Attempts, firstLine(d.LastError))
		}
	case "requeue":
		id := fs.Int64("id", 0, "id dead-задачи")
		if err := fs.Parse(args); err != nil {
			return err
		}
		pool, err := connect(ctx, c, 0)
		if err != nil {
			return err
		}
		defer pool.Close()
		ok, err := jobq.Requeue(ctx, pool, *id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("задача %d не найдена среди dead", *id)
		}
		fmt.Println("возвращена в очередь")
	case "bench":
		return bench(ctx, fs, c, args)
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("неизвестная команда %q", cmd)
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func work(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	conc := fs.Int("concurrency", 4, "одновременных задач")
	lease := fs.Duration("lease", 30*time.Second, "срок аренды")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pool, err := connect(ctx, c, int32(*conc)+4)
	if err != nil {
		return err
	}
	defer pool.Close()
	ts := func() string { return time.Now().Format("15:04:05") }
	w := jobq.NewWorker(jobq.Config{Pool: pool, Queue: c.queue, Concurrency: *conc, Lease: *lease,
		OnResult: func(j jobq.Job, err error, dead bool) {
			switch {
			case err == nil:
				fmt.Printf("%s #%d %s: выполнена (попытка %d)\n", ts(), j.ID, j.Kind, j.Attempts)
			case dead:
				fmt.Printf("%s #%d %s: DEAD после %d попыток: %v\n", ts(), j.ID, j.Kind, j.Attempts, err)
			default:
				fmt.Printf("%s #%d %s: ошибка, повтор позже (попытка %d): %v\n", ts(), j.ID, j.Kind, j.Attempts, err)
			}
		}})
	w.Handle("echo", func(_ context.Context, j jobq.Job) error { fmt.Printf("  echo: %s\n", j.Payload); return nil })
	w.Handle("sleep", func(ctx context.Context, j jobq.Job) error {
		var p struct{ MS int }
		if err := j.Decode(&p); err != nil {
			return jobq.Permanent(err)
		}
		select {
		case <-time.After(time.Duration(p.MS) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	w.Handle("fail", func(context.Context, jobq.Job) error { return errors.New("всегда падает") })
	w.Handle("flaky", func(_ context.Context, j jobq.Job) error {
		if j.Attempts < 3 {
			return fmt.Errorf("попытка %d неудачна", j.Attempts)
		}
		return nil
	})
	fmt.Printf("воркер запущен: очередь=%s, параллельность=%d (Ctrl+C — остановка)\n", c.queue, *conc)
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func bench(ctx context.Context, fs *flag.FlagSet, c *common, args []string) error {
	jobs := fs.Int("jobs", 20000, "сколько задач")
	workers := fs.Int("workers", 4, "воркеров (в одном процессе, у каждого свой пул соединений)")
	conc := fs.Int("concurrency", 16, "задач одновременно на воркер")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c.queue = fmt.Sprintf("bench-%d", time.Now().UnixNano())
	pool, err := connect(ctx, c, 32)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := jobq.Migrate(ctx, pool); err != nil {
		return err
	}

	// 1) скорость постановки: 16 параллельных клиентов
	start := time.Now()
	var next atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= int64(*jobs) {
				if _, _, err := jobq.Enqueue(ctx, pool, "noop", nil, jobq.Queue(c.queue)); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	enq := time.Since(start)

	// 2) скорость обработки пустых задач несколькими воркерами
	var done atomic.Int64
	wctx, stop := context.WithCancel(ctx)
	var wwg sync.WaitGroup
	start = time.Now()
	for i := 0; i < *workers; i++ {
		wp, err := connect(ctx, c, int32(*conc)+4)
		if err != nil {
			stop()
			return err
		}
		defer wp.Close()
		w := jobq.NewWorker(jobq.Config{Pool: wp, Queue: c.queue, Concurrency: *conc, Lease: 30 * time.Second, PollEvery: 20 * time.Millisecond})
		w.Handle("noop", func(context.Context, jobq.Job) error { done.Add(1); return nil })
		wwg.Add(1)
		go func() { defer wwg.Done(); _ = w.Run(wctx) }()
	}
	for done.Load() < int64(*jobs) && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	proc := time.Since(start)
	stop()
	wwg.Wait()
	_, _ = pool.Exec(context.Background(), "delete from jobq_jobs where queue = $1", c.queue)

	fmt.Printf("задач: %d, воркеров: %d × %d\n", *jobs, *workers, *conc)
	fmt.Printf("постановка:  %8.0f задач/с  (%v)\n", float64(*jobs)/enq.Seconds(), enq.Round(time.Millisecond))
	fmt.Printf("обработка:   %8.0f задач/с  (%v)\n", float64(*jobs)/proc.Seconds(), proc.Round(time.Millisecond))
	return nil
}

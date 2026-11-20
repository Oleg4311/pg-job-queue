package jobq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Oleg4311/pg-job-queue/internal/pgtest"
	"github.com/jackc/pgx/v5/pgxpool"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	pool, stop, err := pgtest.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "не удалось подготовить Postgres:", err)
		os.Exit(1)
	}
	testPool = pool
	if err := Migrate(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "миграция:", err)
		stop()
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}

// clean очищает таблицу перед тестом (тесты идут последовательно).
func clean(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	if _, err := testPool.Exec(ctx, `truncate jobq_jobs restart identity`); err != nil {
		t.Fatal(err)
	}
	return ctx
}

func stateOf(t *testing.T, ctx context.Context, id int64) (state string, attempts int, lastErr string) {
	t.Helper()
	var le *string
	if err := testPool.QueryRow(ctx, `select state, attempts, last_error from jobq_jobs where id = $1`, id).Scan(&state, &attempts, &le); err != nil {
		t.Fatal(err)
	}
	if le != nil {
		lastErr = *le
	}
	return
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("не дождались: %s", what)
}

func fastWorker(queue string, conc int) *Worker {
	return NewWorker(Config{
		Pool: testPool, Queue: queue, Concurrency: conc,
		Lease: 2 * time.Second, PollEvery: 50 * time.Millisecond, ReapEvery: 100 * time.Millisecond,
		Backoff: func(int) time.Duration { return 10 * time.Millisecond },
	})
}

func TestMigrateIsIdempotentAndConcurrent(t *testing.T) {
	ctx := clean(t)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- Migrate(ctx, testPool) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := testPool.QueryRow(ctx, `select count(*) from jobq_migrations`).Scan(&n); err != nil || n != len(migrations) {
		t.Fatalf("версий миграций %d (ожидали %d), err=%v", n, len(migrations), err)
	}
}

func TestDedupeKey(t *testing.T) {
	ctx := clean(t)
	id1, ins1, err := Enqueue(ctx, testPool, "mail", map[string]int{"n": 1}, DedupeKey("welcome:42"))
	if err != nil || !ins1 {
		t.Fatal(id1, ins1, err)
	}
	id2, ins2, err := Enqueue(ctx, testPool, "mail", map[string]int{"n": 2}, DedupeKey("welcome:42"))
	if err != nil || ins2 || id2 != id1 {
		t.Fatalf("дубль должен вернуть id первой задачи: id1=%d id2=%d inserted=%v err=%v", id1, id2, ins2, err)
	}
	// другой ключ или другая очередь — не дубль
	if _, ins, _ := Enqueue(ctx, testPool, "mail", nil, DedupeKey("welcome:43")); !ins {
		t.Fatal("другой ключ должен вставляться")
	}
	if _, ins, _ := Enqueue(ctx, testPool, "mail", nil, DedupeKey("welcome:42"), Queue("other")); !ins {
		t.Fatal("другая очередь должна вставляться")
	}
	// после завершения тот же ключ снова можно ставить
	jobs, err := fetch(ctx, testPool, "default", "w", 10, time.Minute)
	if err != nil || len(jobs) != 2 {
		t.Fatal(len(jobs), err)
	}
	for _, j := range jobs {
		if err := complete(ctx, testPool, j); err != nil {
			t.Fatal(err)
		}
	}
	if _, ins, _ := Enqueue(ctx, testPool, "mail", nil, DedupeKey("welcome:42")); !ins {
		t.Fatal("после завершения ключ должен освобождаться")
	}
}

func TestEnqueueInTransactionRollsBack(t *testing.T) {
	ctx := clean(t)
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Enqueue(ctx, tx, "mail", nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	s, _ := GetStats(ctx, testPool, "default")
	if s.Queued != 0 {
		t.Fatalf("после отката задач быть не должно, а их %d", s.Queued)
	}

	tx, _ = testPool.Begin(ctx)
	_, _, _ = Enqueue(ctx, tx, "mail", nil)
	if s, _ := GetStats(ctx, testPool, "default"); s.Queued != 0 {
		t.Fatal("до COMMIT задача не должна быть видна другим")
	}
	_ = tx.Commit(ctx)
	if s, _ := GetStats(ctx, testPool, "default"); s.Queued != 1 {
		t.Fatalf("после COMMIT ожидали 1 задачу, а их %d", s.Queued)
	}
}

func TestPriorityDelayAndOrder(t *testing.T) {
	ctx := clean(t)
	low, _, _ := Enqueue(ctx, testPool, "k", nil, Priority(0))
	high, _, _ := Enqueue(ctx, testPool, "k", nil, Priority(10))
	low2, _, _ := Enqueue(ctx, testPool, "k", nil, Priority(0))
	_, _, _ = Enqueue(ctx, testPool, "k", nil, Priority(100), Delay(time.Hour)) // ещё не готова

	jobs, err := fetch(ctx, testPool, "default", "w", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for _, j := range jobs {
		got = append(got, j.ID)
	}
	want := []int64{high, low, low2}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("порядок выдачи %v, ожидали %v (приоритет, затем FIFO; отложенная не выдаётся)", got, want)
	}
}

// Главное свойство: при гонке многих воркеров каждая задача выполняется ровно один раз.
func TestExactlyOnceUnderConcurrency(t *testing.T) {
	ctx := clean(t)
	const total = 1500
	for i := 0; i < total; i++ {
		if _, _, err := Enqueue(ctx, testPool, "count", map[string]int{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	runs := map[int64]int{}
	var processed atomic.Int64

	wctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		worker := fastWorker("default", 8)
		worker.cfg.WorkerID = fmt.Sprintf("w%d", w)
		worker.Handle("count", func(ctx context.Context, j Job) error {
			mu.Lock()
			runs[j.ID]++
			mu.Unlock()
			processed.Add(1)
			return nil
		})
		wg.Add(1)
		go func() { defer wg.Done(); _ = worker.Run(wctx) }()
	}
	eventually(t, 40*time.Second, "обработку всех задач", func() bool { return processed.Load() >= total })
	stop()
	wg.Wait()

	if len(runs) != total {
		t.Fatalf("выполнено уникальных задач %d из %d", len(runs), total)
	}
	for id, n := range runs {
		if n != 1 {
			t.Fatalf("задача %d выполнена %d раз", id, n)
		}
	}
	s, _ := GetStats(ctx, testPool, "default")
	if s.Done != total || s.Queued != 0 || s.Running != 0 || s.Dead != 0 {
		t.Fatalf("итоговое состояние %+v", s)
	}
}

func TestRetriesThenDead(t *testing.T) {
	ctx := clean(t)
	id, _, _ := Enqueue(ctx, testPool, "flaky", nil, MaxAttempts(3))
	var calls atomic.Int32
	w := fastWorker("default", 1)
	w.Handle("flaky", func(context.Context, Job) error {
		calls.Add(1)
		return errors.New("сломалось")
	})
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = w.Run(wctx) }()
	eventually(t, 10*time.Second, "переход в dead", func() bool { s, _, _ := stateOf(t, ctx, id); return s == "dead" })
	_, attempts, lastErr := stateOf(t, ctx, id)
	if attempts != 3 || calls.Load() != 3 || lastErr != "сломалось" {
		t.Fatalf("attempts=%d calls=%d last_error=%q", attempts, calls.Load(), lastErr)
	}
}

func TestRetrySucceedsOnSecondAttempt(t *testing.T) {
	ctx := clean(t)
	id, _, _ := Enqueue(ctx, testPool, "flaky", nil)
	var calls atomic.Int32
	w := fastWorker("default", 1)
	w.Handle("flaky", func(context.Context, Job) error {
		if calls.Add(1) == 1 {
			return errors.New("первая попытка неудачна")
		}
		return nil
	})
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = w.Run(wctx) }()
	eventually(t, 10*time.Second, "успех со второй попытки", func() bool { s, _, _ := stateOf(t, ctx, id); return s == "done" })
	if _, attempts, _ := stateOf(t, ctx, id); attempts != 2 {
		t.Fatalf("attempts=%d, ожидали 2", attempts)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	ctx := clean(t)
	id, _, _ := Enqueue(ctx, testPool, "bad", nil, MaxAttempts(10))
	w := fastWorker("default", 1)
	w.Handle("bad", func(context.Context, Job) error { return Permanent(errors.New("письмо без адреса")) })
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = w.Run(wctx) }()
	eventually(t, 10*time.Second, "dead", func() bool { s, _, _ := stateOf(t, ctx, id); return s == "dead" })
	if _, attempts, _ := stateOf(t, ctx, id); attempts != 1 {
		t.Fatalf("постоянная ошибка не должна повторяться: attempts=%d", attempts)
	}
}

func TestUnknownKindAndPanic(t *testing.T) {
	ctx := clean(t)
	unknown, _, _ := Enqueue(ctx, testPool, "nobody-handles", nil)
	boom, _, _ := Enqueue(ctx, testPool, "boom", nil, MaxAttempts(1))
	w := fastWorker("default", 2)
	w.Handle("boom", func(context.Context, Job) error { panic("авария") })
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = w.Run(wctx) }()
	eventually(t, 10*time.Second, "обе dead", func() bool {
		a, _, _ := stateOf(t, ctx, unknown)
		b, _, _ := stateOf(t, ctx, boom)
		return a == "dead" && b == "dead"
	})
	if _, _, le := stateOf(t, ctx, boom); !strings.HasPrefix(le, "паника") {
		t.Fatalf("паника должна попасть в last_error, а там %q", le)
	}
}

// Воркер упал: аренда истекла, задачу забирает другой, а «зомби» не может её завершить.
func TestLeaseExpiryReapAndFencing(t *testing.T) {
	ctx := clean(t)
	id, _, _ := Enqueue(ctx, testPool, "slow", nil)

	first, err := fetch(ctx, testPool, "default", "zombie", 1, 300*time.Millisecond)
	if err != nil || len(first) != 1 {
		t.Fatal(len(first), err)
	}
	if n, _ := Reap(ctx, testPool); n != 0 {
		t.Fatal("Reap не должен трогать задачу с действующей арендой")
	}
	time.Sleep(400 * time.Millisecond)
	if n, err := Reap(ctx, testPool); err != nil || n != 1 {
		t.Fatalf("Reap вернул n=%d err=%v, ожидали 1", n, err)
	}
	second, _ := fetch(ctx, testPool, "default", "healthy", 1, time.Minute)
	if len(second) != 1 || second[0].Attempts != 2 {
		t.Fatalf("вторая выдача: %+v", second)
	}
	// зомби просыпается и пытается завершить свою (старую) попытку
	if err := complete(ctx, testPool, first[0]); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("complete зомби должен вернуть ErrLeaseLost, а вернул %v", err)
	}
	if err := extend(ctx, testPool, first[0], time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("extend зомби должен вернуть ErrLeaseLost, а вернул %v", err)
	}
	if state, _, _ := stateOf(t, ctx, id); state != "running" {
		t.Fatalf("задача должна оставаться у второго воркера, а состояние %s", state)
	}
	if err := complete(ctx, testPool, second[0]); err != nil {
		t.Fatal(err)
	}
}

func TestReapKillsExhaustedJob(t *testing.T) {
	ctx := clean(t)
	id, _, _ := Enqueue(ctx, testPool, "x", nil, MaxAttempts(1))
	_, _ = fetch(ctx, testPool, "default", "w", 1, 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	_, _ = Reap(ctx, testPool)
	if s, _, _ := stateOf(t, ctx, id); s != "dead" {
		t.Fatalf("задача с исчерпанными попытками должна стать dead, а %s", s)
	}
	if ok, _ := Requeue(ctx, testPool, id); !ok {
		t.Fatal("Requeue должен вернуть dead-задачу в очередь")
	}
	if s, a, _ := stateOf(t, ctx, id); s != "queued" || a != 0 {
		t.Fatalf("после Requeue: %s attempts=%d", s, a)
	}
}

// Долгая задача (дольше аренды) не должна выполняться дважды: heartbeat продлевает аренду.
func TestHeartbeatKeepsLeaseAlive(t *testing.T) {
	ctx := clean(t)
	id, _, _ := Enqueue(ctx, testPool, "long", nil)
	var calls atomic.Int32
	w := NewWorker(Config{Pool: testPool, Concurrency: 1, Lease: 600 * time.Millisecond, PollEvery: 50 * time.Millisecond, ReapEvery: 100 * time.Millisecond})
	w.Handle("long", func(ctx context.Context, j Job) error {
		calls.Add(1)
		time.Sleep(2 * time.Second) // втрое дольше аренды
		return nil
	})
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = w.Run(wctx) }()
	eventually(t, 15*time.Second, "завершение долгой задачи", func() bool { s, _, _ := stateOf(t, ctx, id); return s == "done" })
	if calls.Load() != 1 {
		t.Fatalf("обработчик вызван %d раз, ожидали 1", calls.Load())
	}
}

// После отмены контекста воркер дорабатывает взятые задачи и фиксирует результат.
func TestGracefulShutdownFinishesInFlight(t *testing.T) {
	ctx := clean(t)
	id, _, _ := Enqueue(ctx, testPool, "k", nil)
	started := make(chan struct{})
	w := fastWorker("default", 1)
	w.Handle("k", func(context.Context, Job) error {
		close(started)
		time.Sleep(500 * time.Millisecond)
		return nil
	})
	wctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	<-started
	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run не завершился")
	}
	if s, _, _ := stateOf(t, ctx, id); s != "done" {
		t.Fatalf("задача в полёте должна дойти до done, а состояние %s", s)
	}
}

// Уведомление будит воркер сразу, даже если опрос раз в час.
func TestNotifyWakesWorker(t *testing.T) {
	ctx := clean(t)
	w := NewWorker(Config{Pool: testPool, Concurrency: 1, Lease: 5 * time.Second, PollEvery: time.Hour})
	got := make(chan struct{}, 1)
	w.Handle("k", func(context.Context, Job) error { got <- struct{}{}; return nil })
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = w.Run(wctx) }()
	time.Sleep(500 * time.Millisecond) // дать воркеру подписаться
	start := time.Now()
	if _, _, err := Enqueue(ctx, testPool, "k", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
		t.Logf("задача подхвачена за %v", time.Since(start))
	case <-time.After(3 * time.Second):
		t.Fatal("воркер не проснулся по NOTIFY")
	}
}

func TestExpBackoffBounds(t *testing.T) {
	b := ExpBackoff(time.Second, 30*time.Second)
	for attempt, base := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 10: 30 * time.Second} {
		for i := 0; i < 200; i++ {
			d := b(attempt)
			if d < base*3/4 || d > base*5/4 {
				t.Fatalf("attempt=%d: %v вне [%v, %v]", attempt, d, base*3/4, base*5/4)
			}
		}
	}
}

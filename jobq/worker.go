package jobq

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler обрабатывает задачу. Возврат nil — успех; ошибка — повтор с задержкой (или dead, если попытки кончились).
// ctx отменяется при остановке воркера и при потере аренды.
type Handler func(ctx context.Context, job Job) error

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent помечает ошибку как неисправимую: задача сразу уходит в dead без повторов.
func Permanent(err error) error { return permanentError{err} }

// Backoff возвращает задержку перед попыткой номер attempt (1 — после первой неудачи).
type Backoff func(attempt int) time.Duration

// ExpBackoff — экспоненциальная задержка base·2^(n-1), не больше max, с «полным» джиттером ±25%.
func ExpBackoff(base, max time.Duration) Backoff {
	return func(attempt int) time.Duration {
		d := base
		for i := 1; i < attempt && d < max; i++ {
			d *= 2
		}
		if d > max {
			d = max
		}
		jitter := 0.75 + rand.Float64()*0.5
		return time.Duration(float64(d) * jitter)
	}
}

// Config настраивает воркер.
type Config struct {
	Pool        *pgxpool.Pool
	Queue       string        // по умолчанию "default"
	WorkerID    string        // по умолчанию worker-<случайный суффикс>
	Concurrency int           // одновременных задач, по умолчанию 4
	Lease       time.Duration // срок аренды, по умолчанию 30 с; продлевается каждые Lease/3
	PollEvery   time.Duration // опрос на случай потерянных уведомлений, по умолчанию 2 с
	ReapEvery   time.Duration // как часто возвращать задачи с истёкшей арендой, по умолчанию Lease/2
	Backoff     Backoff       // по умолчанию ExpBackoff(1с, 5 мин)
	// Hooks — необязательные наблюдатели (метрики, логи).
	OnResult func(job Job, err error, dead bool)
}

// Worker выбирает задачи из очереди и вызывает обработчики по полю kind.
type Worker struct {
	cfg      Config
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewWorker создаёт воркер; Handle регистрирует обработчики до Run.
func NewWorker(cfg Config) *Worker {
	if cfg.Queue == "" {
		cfg.Queue = "default"
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = fmt.Sprintf("worker-%06x", rand.IntN(1<<24))
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 2 * time.Second
	}
	if cfg.ReapEvery <= 0 {
		cfg.ReapEvery = cfg.Lease / 2
	}
	if cfg.Backoff == nil {
		cfg.Backoff = ExpBackoff(time.Second, 5*time.Minute)
	}
	return &Worker{cfg: cfg, handlers: map[string]Handler{}}
}

func (w *Worker) Handle(kind string, h Handler) {
	w.mu.Lock()
	w.handlers[kind] = h
	w.mu.Unlock()
}

// Run работает, пока ctx не отменён. После отмены новые задачи не берутся, а текущие дорабатывают;
// недоделанные задачи вернёт в очередь Reap по истечении аренды.
func (w *Worker) Run(ctx context.Context) error {
	wake := make(chan struct{}, 1)
	go w.listen(ctx, wake)
	go w.reapLoop(ctx)

	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	// jobCtx отменяется, когда мы закончили ждать «хвост» после остановки
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	timer := time.NewTimer(0)
	defer timer.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-wake:
		case <-timer.C:
		}
		// берём ровно столько задач, сколько есть свободных слотов
		for {
			free := w.cfg.Concurrency - len(sem)
			if free <= 0 {
				break
			}
			jobs, err := fetch(ctx, w.cfg.Pool, w.cfg.Queue, w.cfg.WorkerID, free, w.cfg.Lease)
			if err != nil {
				if ctx.Err() != nil {
					break loop
				}
				break // временная ошибка БД: повторим по таймеру
			}
			for _, j := range jobs {
				sem <- struct{}{}
				wg.Add(1)
				go func() {
					defer func() { <-sem; wg.Done(); w.poke(wake) }()
					w.run(jobCtx, j)
				}()
			}
			if len(jobs) < free {
				break
			}
		}
		timer.Reset(w.cfg.PollEvery)
	}

	// даём текущим задачам доработать не дольше одной аренды, потом отменяем их контекст
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.cfg.Lease):
		cancelJobs()
		<-done
	}
	return ctx.Err()
}

func (w *Worker) poke(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (w *Worker) run(ctx context.Context, j Job) {
	w.mu.RLock()
	h, ok := w.handlers[j.Kind]
	w.mu.RUnlock()

	// контекст задачи отменяется, если потеряна аренда
	jctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.cfg.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if err := extend(context.WithoutCancel(ctx), w.cfg.Pool, j, w.cfg.Lease); errors.Is(err, ErrLeaseLost) {
					cancel(ErrLeaseLost)
					return
				}
			}
		}
	}()

	var err error
	if !ok {
		err = Permanent(fmt.Errorf("нет обработчика для kind=%q", j.Kind))
	} else {
		err = safeCall(jctx, h, j)
	}
	close(stop)
	<-hbDone

	// результат фиксируем независимо от отмены ctx, чтобы не терять работу при остановке
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer rcancel()
	var dead bool
	if err == nil {
		err = complete(rctx, w.cfg.Pool, j)
		if err != nil && !errors.Is(err, ErrLeaseLost) {
			return // БД недоступна: задачу вернёт Reap
		}
	} else {
		var perm permanentError
		isPerm := errors.As(err, &perm)
		d, ferr := fail(rctx, w.cfg.Pool, j, err, w.cfg.Backoff(j.Attempts), isPerm)
		dead = d
		if ferr != nil && !errors.Is(ferr, ErrLeaseLost) {
			return
		}
	}
	if w.cfg.OnResult != nil {
		w.cfg.OnResult(j, err, dead)
	}
}

func safeCall(ctx context.Context, h Handler, j Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("паника в обработчике: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, j)
}

func (w *Worker) reapLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = Reap(ctx, w.cfg.Pool)
		}
	}
}

// listen подписывается на NOTIFY, чтобы будить воркер сразу после Enqueue, а не ждать опроса.
func (w *Worker) listen(ctx context.Context, wake chan struct{}) {
	for ctx.Err() == nil {
		conn, err := w.cfg.Pool.Acquire(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		_, err = conn.Exec(ctx, "listen "+pgx.Identifier{notifyChannel(w.cfg.Queue)}.Sanitize())
		for err == nil {
			_, err = conn.Conn().WaitForNotification(ctx)
			if err == nil {
				w.poke(wake)
			}
		}
		// соединение с активным LISTEN нельзя возвращать в пул — закрываем его
		_ = conn.Hijack().Close(context.WithoutCancel(ctx))
		if ctx.Err() != nil {
			return
		}
		time.Sleep(time.Second)
	}
}

// Пакет jobq — очередь фоновых задач поверх PostgreSQL.
//
// Выдача задач идёт через FOR UPDATE SKIP LOCKED: несколько воркеров берут разные строки и не ждут друг друга.
// Взятая задача арендуется (lease): если воркер умер, аренда истекает и задачу подхватывает другой.
// Завершение защищено «отпечатком» попытки: воркер с протухшей арендой не может испортить результат нового.
package jobq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// State — состояние задачи.
type State string

const (
	Queued  State = "queued"
	Running State = "running"
	Done    State = "done"
	Dead    State = "dead" // исчерпаны попытки или ошибка помечена как постоянная
)

// Job — задача, выданная воркеру.
type Job struct {
	ID          int64
	Queue       string
	Kind        string
	Payload     json.RawMessage
	Priority    int
	Attempts    int // номер текущей попытки, начиная с 1
	MaxAttempts int
	LastError   string
	CreatedAt   time.Time
}

// Decode разбирает payload в v.
func (j Job) Decode(v any) error { return json.Unmarshal(j.Payload, v) }

// DB — то, что умеют и пул, и транзакция pgx. Через него Enqueue работает в любой из них.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var (
	// ErrLeaseLost — аренда задачи потеряна (истекла и задачу забрал другой воркер).
	ErrLeaseLost = errors.New("jobq: аренда задачи потеряна")
)

type enqueueOpts struct {
	queue       string
	priority    int
	delay       time.Duration
	maxAttempts int
	dedupeKey   string
}

// EnqueueOption настраивает постановку в очередь.
type EnqueueOption func(*enqueueOpts)

func Queue(name string) EnqueueOption     { return func(o *enqueueOpts) { o.queue = name } }
func Priority(p int) EnqueueOption        { return func(o *enqueueOpts) { o.priority = p } }
func Delay(d time.Duration) EnqueueOption { return func(o *enqueueOpts) { o.delay = d } }
func MaxAttempts(n int) EnqueueOption     { return func(o *enqueueOpts) { o.maxAttempts = n } }
func DedupeKey(k string) EnqueueOption    { return func(o *enqueueOpts) { o.dedupeKey = k } }

// Enqueue ставит задачу в очередь. db может быть пулом или транзакцией: во втором случае задача
// появится только если транзакция зафиксирована (паттерн transactional outbox — без «записали в БД,
// но не поставили задачу» и наоборот).
//
// inserted=false означает, что живая задача с таким DedupeKey уже есть; тогда id — её идентификатор.
func Enqueue(ctx context.Context, db DB, kind string, payload any, opts ...EnqueueOption) (id int64, inserted bool, err error) {
	o := enqueueOpts{queue: "default", maxAttempts: 5}
	for _, f := range opts {
		f(&o)
	}
	if kind == "" {
		return 0, false, errors.New("jobq: пустой kind")
	}
	if o.maxAttempts < 1 {
		return 0, false, errors.New("jobq: MaxAttempts должен быть >= 1")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, false, fmt.Errorf("jobq: payload: %w", err)
	}
	var dedupe *string
	if o.dedupeKey != "" {
		dedupe = &o.dedupeKey
	}

	const q = `
		insert into jobq_jobs (queue, kind, payload, priority, run_at, max_attempts, dedupe_key)
		values ($1, $2, $3, $4, now() + make_interval(secs => $5), $6, $7)
		on conflict (queue, dedupe_key) where dedupe_key is not null and state in ('queued','running') do nothing
		returning id`
	err = db.QueryRow(ctx, q, o.queue, kind, raw, o.priority, o.delay.Seconds(), o.maxAttempts, dedupe).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// конфликт по dedupe_key: вернём id уже существующей задачи
		err = db.QueryRow(ctx, `select id from jobq_jobs where queue = $1 and dedupe_key = $2 and state in ('queued','running')`, o.queue, o.dedupeKey).Scan(&id)
		return id, false, err
	}
	if err != nil {
		return 0, false, err
	}
	// Внутри транзакции уведомление доставится только после COMMIT: воркеры не проснутся зря.
	if _, err := db.Exec(ctx, `select pg_notify($1, '')`, notifyChannel(o.queue)); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func notifyChannel(queue string) string { return "jobq_" + queue }

// fetch выдаёт до limit готовых задач и арендует их на lease.
func fetch(ctx context.Context, db DB, queue, worker string, limit int, lease time.Duration) ([]Job, error) {
	const q = `
		with next as (
			select id from jobq_jobs
			where queue = $1 and state = 'queued' and run_at <= now()
			order by priority desc, run_at, id
			limit $2
			for update skip locked
		)
		update jobq_jobs j
		set state = 'running', attempts = j.attempts + 1, locked_by = $3,
		    locked_until = now() + make_interval(secs => $4)
		from next where j.id = next.id
		returning j.id, j.queue, j.kind, j.payload, j.priority, j.attempts, j.max_attempts, coalesce(j.last_error, ''), j.created_at`
	rows, err := queryRows(ctx, db, q, queue, limit, worker, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Queue, &j.Kind, &j.Payload, &j.Priority, &j.Attempts, &j.MaxAttempts, &j.LastError, &j.CreatedAt); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// rowsQuerier — подмножество pgx, нужное для чтения нескольких строк.
type rowsQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func queryRows(ctx context.Context, db DB, sql string, args ...any) (pgx.Rows, error) {
	rq, ok := db.(rowsQuerier)
	if !ok {
		return nil, errors.New("jobq: db не поддерживает Query")
	}
	return rq.Query(ctx, sql, args...)
}

// complete помечает задачу выполненной. Срабатывает, только если текущая попытка — ещё «наша».
func complete(ctx context.Context, db DB, j Job) error {
	tag, err := db.Exec(ctx, `
		update jobq_jobs set state = 'done', finished_at = now(), locked_by = null, locked_until = null
		where id = $1 and attempts = $2 and state = 'running'`, j.ID, j.Attempts)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// fail записывает ошибку: либо возвращает задачу в очередь с задержкой, либо отправляет в dead.
func fail(ctx context.Context, db DB, j Job, cause error, retryIn time.Duration, permanent bool) (dead bool, err error) {
	err = db.QueryRow(ctx, `
		update jobq_jobs
		set state = case when $5 or attempts >= max_attempts then 'dead' else 'queued' end,
		    run_at = now() + make_interval(secs => $4),
		    last_error = $3,
		    locked_by = null, locked_until = null,
		    finished_at = case when $5 or attempts >= max_attempts then now() end
		where id = $1 and attempts = $2 and state = 'running'
		returning state = 'dead'`, j.ID, j.Attempts, truncate(cause.Error(), 2000), retryIn.Seconds(), permanent).Scan(&dead)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrLeaseLost
	}
	return dead, err
}

// extend продлевает аренду (heartbeat долгих задач).
func extend(ctx context.Context, db DB, j Job, lease time.Duration) error {
	tag, err := db.Exec(ctx, `
		update jobq_jobs set locked_until = now() + make_interval(secs => $3)
		where id = $1 and attempts = $2 and state = 'running'`, j.ID, j.Attempts, lease.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Reap возвращает в очередь задачи, чья аренда истекла (воркер упал или завис). Возвращает число задач.
// Если попытки закончились — задача уходит в dead.
func Reap(ctx context.Context, db DB) (int64, error) {
	tag, err := db.Exec(ctx, `
		update jobq_jobs
		set state = case when attempts >= max_attempts then 'dead' else 'queued' end,
		    run_at = now(), last_error = 'аренда истекла: воркер не ответил',
		    locked_by = null, locked_until = null,
		    finished_at = case when attempts >= max_attempts then now() end
		where state = 'running' and locked_until < now()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Stats — сводка по очереди.
type Stats struct {
	Queued, Running, Done, Dead int64
	OldestQueuedAge             time.Duration // как давно ждёт самая старая готовая задача
}

// GetStats считает состояния задач очереди.
func GetStats(ctx context.Context, db DB, queue string) (Stats, error) {
	var s Stats
	var age float64
	err := db.QueryRow(ctx, `
		select
			count(*) filter (where state = 'queued'),
			count(*) filter (where state = 'running'),
			count(*) filter (where state = 'done'),
			count(*) filter (where state = 'dead'),
			coalesce(extract(epoch from now() - min(run_at) filter (where state = 'queued' and run_at <= now())), 0)
		from jobq_jobs where queue = $1`, queue).Scan(&s.Queued, &s.Running, &s.Done, &s.Dead, &age)
	s.OldestQueuedAge = time.Duration(age * float64(time.Second))
	return s, err
}

// Requeue возвращает мёртвую задачу в очередь (ручной разбор «кладбища»).
func Requeue(ctx context.Context, db DB, id int64) (bool, error) {
	tag, err := db.Exec(ctx, `
		update jobq_jobs set state = 'queued', attempts = 0, run_at = now(), finished_at = null, last_error = null
		where id = $1 and state = 'dead'`, id)
	return tag.RowsAffected() == 1, err
}

// truncate обрезает сообщение по границе руны, чтобы стек паники не раздувал таблицу.
func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

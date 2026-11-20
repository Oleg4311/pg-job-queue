package jobq

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrations применяются по порядку; номер версии = индекс+1.
var migrations = []string{
	`create table jobq_jobs (
		id           bigserial primary key,
		queue        text        not null default 'default',
		kind         text        not null,
		payload      jsonb       not null default '{}',
		state        text        not null default 'queued' check (state in ('queued','running','done','dead')),
		priority     int         not null default 0,
		run_at       timestamptz not null default now(),
		attempts     int         not null default 0,
		max_attempts int         not null default 5,
		locked_by    text,
		locked_until timestamptz,
		last_error   text,
		dedupe_key   text,
		created_at   timestamptz not null default now(),
		finished_at  timestamptz
	);
	-- индекс только по готовым к выдаче задачам: он остаётся маленьким, даже когда в таблице миллионы завершённых
	create index jobq_ready on jobq_jobs (queue, priority desc, run_at, id) where state = 'queued';
	create index jobq_lease on jobq_jobs (locked_until) where state = 'running';
	-- пока задача жива (в очереди или выполняется), второй такой же dedupe_key не вставить
	create unique index jobq_dedupe on jobq_jobs (queue, dedupe_key)
		where dedupe_key is not null and state in ('queued','running');`,
}

// Migrate создаёт/обновляет схему. Безопасно вызывать одновременно из нескольких процессов:
// миграции сериализуются через advisory-блокировку.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	const lockID = 7_251_903_114 // произвольная константа проекта
	if _, err := conn.Exec(ctx, `select pg_advisory_lock($1)`, lockID); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `select pg_advisory_unlock($1)`, lockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `create table if not exists jobq_migrations (version int primary key, applied_at timestamptz not null default now())`); err != nil {
		return err
	}
	var current int
	if err := conn.QueryRow(ctx, `select coalesce(max(version), 0) from jobq_migrations`).Scan(&current); err != nil {
		return err
	}
	for v := current; v < len(migrations); v++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, migrations[v]); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("миграция %d: %w", v+1, err)
		}
		if _, err := tx.Exec(ctx, `insert into jobq_migrations (version) values ($1)`, v+1); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

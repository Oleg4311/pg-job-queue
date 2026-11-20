# pg-job-queue

[![CI](https://github.com/Oleg4311/pg-job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Oleg4311/pg-job-queue/actions/workflows/ci.yml)

**Очередь фоновых задач на одном PostgreSQL — без Redis и брокеров.** Библиотека на Go (`jobq`) и командная строка `pgjq`.
Задача ставится в той же транзакции, что и ваши данные; воркеры берут задачи через `FOR UPDATE SKIP LOCKED`, арендуют их, повторяют с задержкой и не теряют при падении.

*English: a PostgreSQL-backed job queue for Go — transactional enqueue, `SKIP LOCKED` fetching, leases with heartbeat, retries with exponential backoff and a dead-letter state. Details in the [English section](#english) below.*

## Зачем это нужно

Если у вас уже есть PostgreSQL, отдельная очередь часто лишняя. Но «очередь на таблице» легко сделать неправильно: две копии воркера берут одну задачу, упавший воркер навсегда держит задачу, «поставили задачу, а транзакция откатилась» (или наоборот). Проект решает именно эти случаи, и каждое решение закреплено тестом на настоящем Postgres.

| Проблема | Решение | Чем проверено |
|---|---|---|
| Два воркера берут одну задачу | `FOR UPDATE SKIP LOCKED`: каждый берёт свои строки и не ждёт соседа | 6 воркеров × 8 потоков, 1500 задач: каждая выполнена ровно 1 раз |
| Воркер умер с задачей в руках | аренда (lease); `Reap` возвращает задачу в очередь, когда аренда истекла | тест с истёкшей арендой |
| «Зомби» очнулся и завершил чужую попытку | завершение с проверкой номера попытки (`where attempts = $n`) → `ErrLeaseLost` | мутационная проверка: убрали проверку — тест падает |
| Долгая задача дольше аренды | heartbeat продлевает аренду каждые `Lease/3` | задача в 3 раза дольше аренды выполнена один раз |
| Записали в БД, а задачу не поставили | `Enqueue` принимает транзакцию (transactional outbox) | откат транзакции — задачи нет, коммит — есть |
| Дубликаты (двойной клик, повтор вебхука) | `DedupeKey`: пока задача жива, вторая с тем же ключом не вставится | тест дедупликации |
| Сбои обработчика | повтор с экспоненциальной задержкой и джиттером, потом `dead`; `Permanent(err)` — сразу в `dead` | тесты повторов и постоянной ошибки |
| Паника в обработчике | перехватывается, попадает в `last_error` | тест паники |
| Остановка сервиса | воркер дорабатывает взятые задачи и фиксирует результат | тест остановки |
| Задержка «опроса» | `LISTEN/NOTIFY` будит воркера сразу: ~8 мс вместо периода опроса | тест уведомления при опросе раз в час |

## Быстрый старт

```go
pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
jobq.Migrate(ctx, pool) // безопасно вызывать из нескольких процессов одновременно

// поставить задачу — в одной транзакции с вашими данными
tx, _ := pool.Begin(ctx)
tx.Exec(ctx, `insert into orders (...) values (...)`)
jobq.Enqueue(ctx, tx, "send-receipt", map[string]any{"order": 42},
    jobq.DedupeKey("receipt:42"), jobq.MaxAttempts(8), jobq.Delay(5*time.Second))
tx.Commit(ctx) // задача появится только после коммита

// воркер
w := jobq.NewWorker(jobq.Config{Pool: pool, Concurrency: 8})
w.Handle("send-receipt", func(ctx context.Context, job jobq.Job) error {
    var p struct{ Order int }
    if err := job.Decode(&p); err != nil {
        return jobq.Permanent(err) // повторять бессмысленно
    }
    return sendReceipt(ctx, p.Order) // ошибка → повтор с задержкой
})
w.Run(ctx) // работает до отмены ctx; потом дорабатывает текущие задачи
```

## Командная строка

Скачайте `pgjq` со страницы [Releases](https://github.com/Oleg4311/pg-job-queue/releases) (Linux, macOS, Windows) или соберите: `go install github.com/Oleg4311/pg-job-queue/cmd/pgjq@latest`.

```bash
docker compose up -d                                   # PostgreSQL (compose.yml в репозитории)
export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"

pgjq migrate
pgjq enqueue --kind echo --payload '{"hello":"мир"}'
pgjq enqueue --kind flaky --max-attempts 5             # падает дважды, на 3-й попытке выполняется
pgjq enqueue --kind fail --max-attempts 2              # всегда падает → dead
pgjq work --concurrency 4                              # демо-воркер (echo, sleep, fail, flaky)
pgjq stats
pgjq dead                                              # что и почему умерло
pgjq requeue --id 5                                    # вернуть в очередь после исправления
pgjq bench --jobs 20000 --workers 4 --concurrency 16   # измерить на вашей базе
```

Реальный вывод воркера (так запускалось при разработке):

```
11:39:47 #4 flaky: ошибка, повтор позже (попытка 1): попытка 1 неудачна
11:39:47 #6 nobody: DEAD после 1 попыток: нет обработчика для kind="nobody"
11:39:49 #5 fail: DEAD после 2 попыток: всегда падает
11:39:53 #4 flaky: выполнена (попытка 3)
```

Без Docker: `go run ./internal/devdb` запустит одноразовый встроенный PostgreSQL и напечатает `DATABASE_URL` (бинарники скачиваются при первом запуске).

## Как это устроено

Одна таблица `jobq_jobs`, состояния `queued → running → done | dead`:

```sql
-- выдача: берём свободные строки, не дожидаясь занятых, и сразу арендуем
with next as (
  select id from jobq_jobs
  where queue = $1 and state = 'queued' and run_at <= now()
  order by priority desc, run_at, id
  limit $2
  for update skip locked
)
update jobq_jobs j set state = 'running', attempts = j.attempts + 1,
       locked_by = $3, locked_until = now() + make_interval(secs => $4)
from next where j.id = next.id returning ...
```

- **Частичные индексы.** Индекс выдачи построен только по `state = 'queued'`, поэтому остаётся маленьким, даже когда в таблице миллионы завершённых задач.
- **Отпечаток попытки.** `attempts` растёт при каждой выдаче, а `complete`/`fail`/`extend` срабатывают только для «своей» попытки. Воркер, у которого аренду отобрали, получает `ErrLeaseLost`, его контекст отменяется.
- **Уведомления внутри транзакции.** `pg_notify` вызывается в той же транзакции, что вставка, поэтому воркеры просыпаются только после коммита.
- **Дедупликация** — частичный уникальный индекс по `(queue, dedupe_key)` для живых задач; после завершения ключ освобождается.

## Производительность

`pgjq bench` на ноутбуке разработчика (Windows, 12 потоков, встроенный Postgres без тюнинга, пустые задачи; одна и та же машина запускает и базу, и клиентов):

| | задач/с |
|---|---|
| постановка (16 параллельных клиентов, по одной задаче на запрос) | ≈ 15 700 |
| обработка (4 воркера × 16 потоков) | ≈ 10 400 |

Тот же бенчмарк на общих раннерах GitHub Actions (2 vCPU, база и клиенты на одной машине, один запуск, без усреднения):

| PostgreSQL | постановка, задач/с | обработка, задач/с |
|---|---|---|
| 14 | ≈ 9 300 | ≈ 9 700 |
| 16 | ≈ 16 100 | ≈ 13 800 |
| 17 | ≈ 7 600 | ≈ 7 400 |

Разброс между версиями — в основном шум общих раннеров, а не свойство версий Postgres: делать выводы «17 медленнее 16» по одному запуску нельзя. Это «пустые» задачи на неотдельной БД, а не обещание: на вашем железе числа будут другими — измерьте командой `pgjq bench`. Цифры каждого запуска CI сохраняются в артефактах (`bench-pg*`).

## Честные ограничения

- **At-least-once.** Если воркер выполнил побочный эффект и упал до фиксации результата, задачу выполнит другой воркер. Делайте обработчики идемпотентными (для этого и нужен `Job.ID` и `DedupeKey`). Выполнения «ровно один раз» очередь на таблице обещать не может.
- **Завершённые задачи не удаляются сами.** Таблица растёт; удаляйте старые `done` по расписанию (`delete from jobq_jobs where state = 'done' and finished_at < now() - interval '7 days'`).
- **Через pgbouncer** в режиме transaction pooling `LISTEN/NOTIFY` не работает: воркер продолжит работать по опросу (`PollEvery`).
- **Пропускная способность** упирается в одну БД. Для сотен тысяч задач в секунду нужны Kafka или NATS.

## Разработка

```bash
go test ./...                    # без DATABASE_URL поднимется встроенный Postgres
DATABASE_URL=... go test -race ./...   # на своём Postgres, с детектором гонок
bash scripts/smoke.sh            # сквозной прогон CLI (нужен DATABASE_URL)
```

CI запускает тесты с `-race` на PostgreSQL 14, 16, 17 (service container), сквозной прогон CLI и бенчмарк, а ещё тесты на Linux/Windows/macOS со встроенным Postgres. Релизы (`v*`) собираются из тега, содержат контрольные суммы и подтверждение происхождения:

```bash
gh attestation verify pgjq-v0.1.0-linux-amd64.tar.gz --repo Oleg4311/pg-job-queue
```

Лицензия: MIT. Сделано с помощью Claude.

---

## English

**pg-job-queue** is a job queue that lives in your PostgreSQL — no Redis, no broker. Go library `jobq` + CLI `pgjq`.

- **Transactional enqueue:** `jobq.Enqueue(ctx, tx, ...)` accepts a pgx transaction, so a job exists iff your business data was committed (outbox pattern).
- **`FOR UPDATE SKIP LOCKED`** fetching: workers never block each other; tested with 6 workers × 8 goroutines over 1500 jobs, each executed exactly once.
- **Leases + heartbeat + fencing:** a dead worker's job is reclaimed by `Reap`; a "zombie" worker cannot complete a newer attempt (`ErrLeaseLost`). Mutation-tested: removing the fence makes the test fail.
- **Retries** with exponential backoff and jitter, `Permanent(err)` to skip retries, a dead-letter state with `pgjq dead` / `pgjq requeue`.
- **`LISTEN/NOTIFY`** wake-ups (~8 ms), polling as a fallback. **Dedupe keys** via a partial unique index.
- Delivery is **at-least-once** — make handlers idempotent.

```bash
docker compose up -d
export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
pgjq migrate && pgjq enqueue --kind echo --payload '{"hi":1}' && pgjq work
```

Release artifacts carry SHA-256 sums and build provenance (`gh attestation verify`). MIT licensed. Built with Claude.

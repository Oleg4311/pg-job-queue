#!/usr/bin/env bash
# Сквозной прогон CLI на настоящем Postgres: миграция, постановка, воркер, ретраи, dead, requeue.
set -euo pipefail
: "${DATABASE_URL:?нужен DATABASE_URL}"
go build -o pgjq ./cmd/pgjq
P=./pgjq
fail() { echo "ПРОВАЛ: $*" >&2; exit 1; }

$P migrate
$P migrate   # повторно: схема уже актуальна
Q="smoke-$$"
$P enqueue --queue "$Q" --kind echo --payload '{"a":1}' | grep -q "поставлена" || fail "enqueue"
$P enqueue --queue "$Q" --kind echo --dedupe k | grep -q "поставлена" || fail "первый dedupe"
$P enqueue --queue "$Q" --kind echo --dedupe k | grep -q "дубль" || fail "дубль не распознан"
$P enqueue --queue "$Q" --kind flaky --max-attempts 5 >/dev/null
$P enqueue --queue "$Q" --kind fail --max-attempts 2 >/dev/null

timeout 15 $P work --queue "$Q" --concurrency 3 --lease 5s > work.log 2>&1 || true
cat work.log
grep -q "flaky: выполнена (попытка 3)" work.log || fail "flaky должна пройти с 3-й попытки"
grep -q "fail: DEAD после 2 попыток" work.log || fail "fail должна уйти в dead"

$P stats --queue "$Q" | tee stats.log
grep -q "queued=0 running=0 done=3 dead=1" stats.log || fail "итоговая статистика"
$P dead --queue "$Q" | grep -q "всегда падает" || fail "dead не показал причину"
id=$($P dead --queue "$Q" | sed -n 's/^#\([0-9]*\) .*/\1/p' | head -1)
$P requeue --queue "$Q" --id "$id" | grep -q "возвращена" || fail "requeue"
$P stats --queue "$Q" | grep -q "queued=1" || fail "после requeue должна быть 1 задача"
echo "smoke: всё хорошо"

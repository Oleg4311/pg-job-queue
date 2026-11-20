package jobq

import "context"

// DeadJob — запись «кладбища» для ручного разбора.
type DeadJob struct {
	ID        int64
	Kind      string
	Attempts  int
	LastError string
}

// ListDead возвращает до limit последних dead-задач очереди.
func ListDead(ctx context.Context, db DB, queue string, limit int) ([]DeadJob, error) {
	rows, err := queryRows(ctx, db, `
		select id, kind, attempts, coalesce(last_error, '') from jobq_jobs
		where queue = $1 and state = 'dead' order by finished_at desc, id desc limit $2`, queue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadJob
	for rows.Next() {
		var d DeadJob
		if err := rows.Scan(&d.ID, &d.Kind, &d.Attempts, &d.LastError); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

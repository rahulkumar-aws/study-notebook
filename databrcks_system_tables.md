In Databricks, job runs and task run states are logged in **system tables** under the `databricks` schema (part of the **Lakehouse Monitoring / System Tables**).
You can query **`system.job_runs`** (for job-level) and **`system.task_runs`** (for task-level).

Here’s a simple query to get failed jobs:

```sql
-- Failed Job Runs
SELECT
  job_id,
  job_name,
  run_id,
  start_time,
  end_time,
  state_lifecycle,
  state_result,
  trigger_type,
  creator_user_name
FROM system.job_runs
WHERE state_result = 'FAILED'
ORDER BY end_time DESC
LIMIT 50;
```

For **failed tasks inside jobs**:

```sql
-- Failed Task Runs
SELECT
  job_id,
  job_name,
  run_id,
  task_key,
  start_time,
  end_time,
  state_lifecycle,
  state_result,
  cluster_id
FROM system.task_runs
WHERE state_result = 'FAILED'
ORDER BY end_time DESC
LIMIT 50;
```

🔎 Notes:

* `state_lifecycle` tells you if it was `TERMINATED`, `INTERNAL_ERROR`, etc.
* `state_result` is the key column (`SUCCESS`, `FAILED`, `CANCELED`).
* `trigger_type` helps you distinguish scheduled vs manual runs.

👉 Do you want me to also give you a **query that aggregates failure counts per job** (so you can see which jobs fail most often)?

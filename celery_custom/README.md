# datadog-celery
Datadog check for celery using the [Flower API](http://flower.readthedocs.org/en/latest/api.html)

## Service checks

- celery.can_connect
  - checks that we can connect to Flower

## Metric list

### Queue
Per queue metrics, read from the broker via Flower's `/api/queues/length`.

Tags:

- celery_queue:{queue name}

Metrics:

- celery.tasks_queued

## Worker and task metrics

This check no longer reports per-worker or per-task metrics. Those are emitted
directly by HQ under `commcare.celery.*`, which covers the same info more
reliably:

- `commcare.celery.task.time_to_run.seconds` — throughput, tagged
  `state:success|failure|retry` and `celery_task_name`
- `commcare.celery.task.time_to_start` — enqueue-to-start latency
- `commcare.celery.heartbeat.*` — per queue liveness and blockage

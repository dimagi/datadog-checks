# datadog-celery
Datadog check for celery using the [Flower API](http://flower.readthedocs.org/en/latest/api.html)

## Service checks

- celery.can_connect
  - checks that we can connect to Flower
- celery.worker_status
  - an individual check for each worker tagged with `celery_worker:{worker name}`

## Metric list

### Worker
Per worker metrics.

Tags:

- celery_worker:{worker name}
  - worker name is just the part after the host which is mostly the same as the queue name
- celery_queue:{queue name}

Metrics:

- celery.tasks_registered
- celery.max-concurrency
- celery.tasks_completed
    - additional tags: celery_task_name:{task name}
  
### Tasks
Per task metrics

Tags:

- celery_worker:{worker name}

Metrics:

- celery.tasks_by_state.{state} 
    - [List of states](http://docs.celeryproject.org/en/latest/userguide/tasks.html#built-in-states)

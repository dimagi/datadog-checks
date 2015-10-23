# datadog-celery
Datadog check for celery using the [Flower API](http://flower.readthedocs.org/en/latest/api.html)

## Metric list

### Worker
Tags:

- celery_worker:{worker name}
- celery_queue:{queue name}

Metrics:

- celery.tasks_registered
- celery.max-concurrency
- celery.tasks_completed
    - additional tags: celery_task_name:{task name}
  
### Tasks
Tags:

- celery_worker:{worker name}

Metrics:

- celery.tasks_by_state.{state} 
    - [List of states](http://docs.celeryproject.org/en/latest/userguide/tasks.html#built-in-states)

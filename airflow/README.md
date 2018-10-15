# datadog-airflow
Datadog check for airflow. Requires SQLAlchemy to be installed with datadog.
See https://docs.datadoghq.com/agent/custom_python_package/?tab=linux for installing custom packages.

## Service checks

- airflow.webserver
  - checks that we can connect to the webserver
- airflow.scheduler
  - checks that the scheduler is running

## Metric list

Metrics:

- airflow.dags_active.total
- airflow.dags_active.paused
- airflow.dags_active.not_paused
- airflow.task_instances.<state>  # Only includes tasks for DAGs that are currently active
- airflow.dag_runs.<state>  # Only includes dag runs for DAGs that are currently active

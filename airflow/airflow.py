import requests
import sqlalchemy

from checks import AgentCheck

AIRFLOW_WEBSERVER_URL_KEY = "airflow_webserver_url"
AIRFLOW_SQL_ALCHEMY_CONN_KEY = "airflow_sql_alchemy_conn"


class AirflowCheck(AgentCheck):
    """Extracts stats from airflow via a sqlalchemy connection"""

    SERVICE_CHECK_NAME = "airflow.webserver"
    SOURCE_TYPE_NAME = "airflow"

    def _validate_instance(self, instance):
        for key in [AIRFLOW_WEBSERVER_URL_KEY, AIRFLOW_WEBSERVER_URL_KEY]:
            if key not in instance:
                raise Exception("A {} must be specified".format(key))

    def check(self, instance):
        self._validate_instance(instance)

        tags = instance.get("tags", [])
        self.check_webserver_connection(instance, tags)
        self.get_dag_data(instance, tags)
        self.get_task_data(instance, tags)
        self.get_dag_run_data(instance, tags)

    def get_dag_data(self, instance, tags):
        engine = sqlalchemy.create_engine(instance[AIRFLOW_SQL_ALCHEMY_CONN_KEY])
        [count] = engine.execute("select count(*) from dag where is_active = true").fetchone()
        [active_and_not_paused_dags] = engine.execute(
            "select count(*) from dag where is_active = true and is_paused = false"
        ).fetchone()
        self.gauge("{}.dags_active.total".format(self.SOURCE_TYPE_NAME), count, tags=tags)
        self.gauge(
            "{}.dags_active.not_paused".format(self.SOURCE_TYPE_NAME), active_and_not_paused_dags, tags=tags
        )
        self.gauge(
            "{}.dags_active.paused".format(self.SOURCE_TYPE_NAME), count - active_and_not_paused_dags, tags=tags
        )

    def get_task_data(self, instance, tags):
        """
        See https://github.com/apache/airflow/blob/1.10.0/airflow/utils/state.py#L49 for available
        task states
        """
        engine = sqlalchemy.create_engine(instance[AIRFLOW_SQL_ALCHEMY_CONN_KEY])
        states = [
            "success",
            "failed",
            "upstream_failed",
            "skipped",
            "running",
            "queued",
        ]
        for state in states:
            [count] = engine.execute(
                """
                select count(*) from task_instance inner join dag on task_instance.dag_id = dag.dag_id
                where dag.is_active = true and state = '{}'
            """.format(
                    state
                )
            ).fetchone()
            self.gauge("{}.task_instances.{}".format(self.SOURCE_TYPE_NAME, state), count, tags=tags)

    def get_dag_run_data(self, instance, tags):
        """
        See https://github.com/apache/airflow/blob/1.10.0/airflow/utils/state.py#L60 for available
        dag states
        """
        engine = sqlalchemy.create_engine(instance[AIRFLOW_SQL_ALCHEMY_CONN_KEY])
        states = [
            "success",
            "failed",
            "running",
        ]
        for state in states:
            [count] = engine.execute(
                """
                select count(*) from dag_run inner join dag on dag_run.dag_id = dag.dag_id
                where dag.is_active = true and state = '{}'
            """.format(
                    state
                )
            ).fetchone()
            self.gauge("{}.dag_runs.{}".format(self.SOURCE_TYPE_NAME, state), count, tags=tags)

    def check_webserver_connection(self, instance, tags):
        url = instance[AIRFLOW_WEBSERVER_URL_KEY]

        try:
            requests.get(url)
        except requests.exceptions.Timeout as e:
            self.service_check(
                self.SERVICE_CHECK_NAME,
                AgentCheck.CRITICAL,
                tags=tags,
                message="Request timeout: {0}, {1}".format(url, e),
            )
            raise
        except requests.exceptions.HTTPError as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, tags=tags, message=str(e.message))
            raise
        except Exception as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, tags=tags, message=str(e))
            raise
        else:
            self.service_check(
                self.SERVICE_CHECK_NAME, AgentCheck.OK, tags=tags, message="Connection to %s was successful" % url
            )

import requests

from datadog_checks.base import AgentCheck
from util import headers
import sys


class CeleryCustom(AgentCheck):
    """Extracts stats from Celery via the Flower REST API
    http://flower.readthedocs.org/en/latest/api.html
    """

    SERVICE_CHECK_NAME = 'celery.can_connect'
    SOURCE_TYPE_NAME = 'celery'
    TIMEOUT = 5
    QUEUE_LENGTH_ENDPOINT = '/api/queues/length'

    def _validate_instance(self, instance):
        for key in ['flower_url']:
            if not key in instance:
                raise Exception(f'A {key} must be specified')

    def _get_response_from_url(self, url, instance, params=None):
        self.log.debug(f'Fetching Celery stats at url: {url}')

        auth=None
        if 'username' and 'password' in instance:
            auth = (instance['username'], instance['password'])

        request_headers = headers(self.agentConfig)
        response = requests.get(url, params=params, auth=auth, headers=request_headers,
                         timeout=int(instance.get('timeout', self.TIMEOUT)))
        response.raise_for_status()
        return response

    def _get_data_from_url(self, url, instance, params=None):
        "Hit a given URL and return the parsed json"
        response = self._get_response_from_url(url, instance, params)
        return response.json()

    def _safe_get_data_from_url(self, url, instance, params=None):
        try:
            data = self._get_data_from_url(url, instance, params)
        except requests.exceptions.HTTPError as e:
            self.warning(f'Error reading data from URL: {url}')
            return

        if data is None:
            self.warning(f'No stats could be retrieved from {url}')

        return data

    def check(self, instance):
        self._validate_instance(instance)

        tags = instance.get('tags', [])
        self.check_connection(instance, tags)

        self.get_tasks_queued_data(instance, tags)

    def check_connection(self, instance, tags):
        url = instance['flower_url']
        try:
            self._get_response_from_url(url, instance)
        except requests.exceptions.Timeout as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                tags=tags, message=f'Request timeout: {url}, {e}')
            raise
        except requests.exceptions.HTTPError as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                tags=tags, message=str(e.message))
            raise
        except Exception as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                tags=tags, message=str(e))
            raise
        else:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK,
                tags=tags,
                message=f'Connection to {url} was successful')

    def get_tasks_queued_data(self, instance, tags):
        url = instance['flower_url'] + self.QUEUE_LENGTH_ENDPOINT
        data = self._safe_get_data_from_url(url, instance)
        for queue in data.get('active_queues'):
            queue_tag = f"celery_queue:{queue.get('name')}"
            self.gauge(
                f'{self.SOURCE_TYPE_NAME}.tasks_queued',
                queue.get('messages'),
                tags=tags + [queue_tag]
            )


if __name__ == '__main__':
    if len(sys.argv) == 2:
        path = sys.argv[1]
    else:
        print("Usage: python celery.py <path_to_config>")
    check, instances = CeleryCustom.from_yaml(path)
    for instance in instances:
        print(f"\nRunning the check against url: {instance['flower_url']}")
        check.check(instance)
        if check.has_events():
            print(f'Events: {check.get_events()}')
        print(f'Metrics: {check.get_metrics()}')

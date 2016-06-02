import requests

from checks import AgentCheck
from util import headers
import sys


class CeleryCheck(AgentCheck):
    """Extracts stats from Celery via the Flower REST API
    http://flower.readthedocs.org/en/latest/api.html
    """

    SERVICE_CHECK_NAME = 'celery.can_connect'
    SOURCE_TYPE_NAME = 'celery'
    TIMEOUT = 5
    URL_ENDPOINTS = {
        'workers': '/api/workers',
        'tasks': '/api/tasks',
        'task_types': '/api/task/types',
    }

    # http://docs.celeryproject.org/en/latest/reference/celery.states.html#misc
    TASK_STATES = (
        'PENDING',
        'RECEIVED',
        'STARTED',
        'SUCCESS',
        'FAILURE',
        'REVOKED',
        'RETRY'
    )

    def __init__(self, name, init_config, agentConfig, instances=None):
        super(CeleryCheck, self).__init__(name, init_config, agentConfig, instances)
        self.last_timestamps = {}

    def _validate_instance(self, instance):
        for key in ['flower_url']:
            if not key in instance:
                raise Exception("A {} must be specified".format(key))

    def _get_response_from_url(self, url, instance, params=None):
        self.log.debug('Fetching Celery stats at url: %s' % url)

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
            self.warning('Error reading data from URL: {}'.format(url))
            return

        if data is None:
            self.warning("No stats could be retrieved from {}".format(url))

        return data

    def check(self, instance):
        self._validate_instance(instance)

        tags = instance.get('tags', [])
        self.check_connection(instance, tags)

        workers = self.get_worker_data(instance, tags)
        self.get_task_data(instance, tags, workers)

    def check_connection(self, instance, tags):
        url = instance['flower_url']
        try:
            self._get_response_from_url(url, instance)
        except requests.exceptions.Timeout as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                tags=tags, message="Request timeout: {0}, {1}".format(url, e))
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
                message='Connection to %s was successful' % url)

    def _split_worker_name(self, worker_name):
        """Assumes worker name formatted as follows: celery@hostname.domain_queue
        """
        host_string = worker_name.split('@', 1)[1]
        hostname = host_string.split('.', 1)[0]
        short_worker_name = host_string.split('_', 1)[1]
        return hostname, short_worker_name

    def get_worker_data(self, instance, tags):
        """
        :return: list of worker names
        """
        data = self._get_data_for_endpoint(instance, 'workers')
        if data is None:
            return []

        status_data = self._get_data_for_endpoint(instance, 'workers', params={'status': True})

        for worker_name, worker_data in data.items():
            hostname, short_worker_name = self._split_worker_name(worker_name)
            queue = worker_data['active_queues'][0]['name']
            worker_tag = 'celery_worker:{}'.format(short_worker_name)
            queue_tag = 'celery_queue:{}'.format(queue)

            self.gauge(
                '{}.tasks_registered'.format(self.SOURCE_TYPE_NAME),
                len(worker_data['registered']),
                hostname=hostname,
                tags=tags + [worker_tag, queue_tag]
            )

            stats = worker_data['stats']
            if stats.get('pool', None):
                self.gauge(
                    '{}.max-concurrency'.format(self.SOURCE_TYPE_NAME),
                    stats['pool']['max-concurrency'],
                    hostname=hostname,
                    tags=tags + [worker_tag, queue_tag]
                )

            for task_name, total in stats['total'].items():
                self.gauge(
                    '{}.tasks_completed'.format(self.SOURCE_TYPE_NAME),
                    total,
                    hostname=hostname,
                    tags=tags + [worker_tag, queue_tag, 'celery_task_name:{}'.format(task_name)]
                )

            status = status_data.get(worker_name, False)
            dd_status = AgentCheck.OK if status else AgentCheck.CRITICAL
            self.service_check(
                '{}.worker_status'.format(self.SOURCE_TYPE_NAME),
                dd_status,
                hostname=hostname,
                tags=tags + [worker_tag]
            )
        return data.keys()

    def get_task_data(self, instance, tags, workers):
        for worker in workers:
            hostname, short_worker_name = self._split_worker_name(worker)
            metric_tags = tags + ['celery_worker:{}'.format(short_worker_name)]
            for state in self.TASK_STATES:

                data = self._get_data_for_endpoint(instance, 'tasks', params={
                    'workername': worker,
                    'state': state
                })

                self.gauge(
                    '{}.tasks_by_state.{}'.format(self.SOURCE_TYPE_NAME, state),
                    len(data),
                    hostname=hostname,
                    tags=metric_tags
                )

    def _get_data_for_endpoint(self, instance, endpoint, params=None):
        url = '{}{}'.format(
            instance['flower_url'],
            self.URL_ENDPOINTS[endpoint]
        )

        return self._safe_get_data_from_url(url, instance, params)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        path = sys.argv[1]
    else:
        print "Usage: python celery.py <path_to_config>"
    check, instances = CeleryCheck.from_yaml(path)
    for instance in instances:
        print "\nRunning the check against url: %s" % (instance['flower_url'])
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())

from collections import Counter
from datetime import datetime
import requests

from checks import AgentCheck
from util import headers
import sys


class CloudantCheck(AgentCheck):
    """Extracts stats from Cloudant via its REST API
    https://docs.cloudant.com/monitoring.html
    """

    SERVICE_CHECK_NAME = 'cloudant.can_connect'
    SOURCE_TYPE_NAME = 'cloudant'
    TIMEOUT = 5
    MONITOR_URL_TEMPLATE = 'https://{username}.cloudant.com/_api/v2/monitoring/{endpoint}?cluster={cluster}'
    ACTIVE_TASKS_URL_TEMPLATE = 'https://{username}.cloudant.com/_active_tasks'

    def __init__(self, name, init_config, agentConfig, instances=None):
        super(CloudantCheck, self).__init__(name, init_config, agentConfig, instances)
        self.last_timestamps = {}

    def _validate_instance(self, instance):
        for key in ['cluster', 'username', 'password']:
            if not key in instance:
                raise Exception("A {} must be specified".format(key))

    def _get_data_from_url(self, url, instance):
        "Hit a given URL and return the parsed json"
        self.log.debug('Fetching Cloudant stats at url: %s' % url)

        auth = (instance['username'], instance['password'])
        # Override Accept request header so that failures are not redirected to the Futon web-ui
        request_headers = headers(self.agentConfig)
        request_headers['Accept'] = 'text/json'
        r = requests.get(url, auth=auth, headers=request_headers,
                         timeout=int(instance.get('timeout', self.TIMEOUT)))
        r.raise_for_status()
        return r.json()

    def _safe_get_data_from_url(self, url, instance):
        try:
            data = self._get_data_from_url(url, instance)
        except requests.exceptions.HTTPError as e:
            self.warning('Error reading data from URL: {}'.format(url))
            return

        if data is None:
            self.warning("No stats could be retrieved from {}".format(url))

        return data

    def check(self, instance):
        self._validate_instance(instance)

        tags = instance.get('tags', [])
        tags.append('cluster:{}'.format(instance['cluster']))
        self.check_connection(instance, tags)

        self.rate_status_code_data(instance, tags)
        self.rate_verb_data(instance, tags)
        self.disk_use_data(instance, tags)
        self.get_data_for_endpoint(instance, 'kv_emits', tags=tags)
        self.get_data_for_endpoint(instance, 'map_doc', tags=tags)
        self.get_data_for_endpoint(instance, 'rps', metric_group='doc_reads', tags=tags)
        self.get_data_for_endpoint(instance, 'wps', metric_group='doc_writes', tags=tags)
        self.active_task_data(instance, tags)

    def check_connection(self, instance, tags):
        url = self.MONITOR_URL_TEMPLATE.format(
            endpoint='uptime',
            **instance
        )
        try:
            self._get_data_from_url(url, instance)
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

    def rate_status_code_data(self, instance, tags):
        self.get_data_for_endpoint(
            instance,
            'rate/status_code',
            lambda target: target.split(' ')[-1],
            metric_group='http_status_code',
            tags=tags
        )

    def rate_verb_data(self, instance, tags):
        self.get_data_for_endpoint(
            instance,
            'rate/verb',
            lambda target: target.split(' ')[-1].lower(),
            metric_group='http_method',
            tags=tags
        )

    def disk_use_data(self, instance, tags):
        def _stat_name(target):
            tokens = target.split(' ')
            assert tokens[0] == instance['cluster']
            return tokens[1].lower()

        self.get_data_for_endpoint(instance, 'disk_use', _stat_name, tags=tags)

    def get_data_for_endpoint(self, instance, endpoint, stat_name_fn=None, metric_group=None, tags=None):
        url = self.MONITOR_URL_TEMPLATE.format(
            endpoint=endpoint,
            **instance
        )

        data = self._safe_get_data_from_url(url, instance)

        metric_group = metric_group or endpoint
        self.record_data(data, metric_group, stat_name_fn, tags)

    def _should_record_data(self, tag, epoch):
        last_ts = self.last_timestamps.get(tag, None)
        return not last_ts or last_ts < epoch

    def record_data(self, data, metric_group, stat_name_fn=None, tags=None):
        end_epoch = data['end']
        prefix = '.'.join([self.SOURCE_TYPE_NAME, metric_group])
        if not self._should_record_data(prefix, end_epoch):
            self.log.info('Skipping old data: {}'.format(prefix))
            return

        granularity = data.get('granularity', '15sec')
        for response in data['target_responses']:
            target = response['target']
            metric_name = '.'.join([prefix, stat_name_fn(target)]) if stat_name_fn else prefix
            datapoints = response['datapoints']
            for datapoint in datapoints:
                value, epoch = datapoint
                value = self._convert_to_per_sec(value, granularity)
                if value is not None and self._should_record_data(metric_name, epoch):
                    self.log.debug('Recording data: {}, {}'.format(metric_name, value))
                    self.last_timestamps[metric_name] = epoch
                    metric_tags = tags or []
                    self.gauge(metric_name, value, tags=metric_tags, timestamp=epoch)

    def active_task_data(self, instance, tags):
        url = self.ACTIVE_TASKS_URL_TEMPLATE.format(
            username=instance['username']
        )

        data = self._safe_get_data_from_url(url, instance)
        cnt = Counter()
        for task in data:
            type_ = task.get('type', None)
            if type_:
                cnt[type_] += 1

        metric_tags = tags or []
        prefix = '{}.tasks'.format(self.SOURCE_TYPE_NAME)
        for task_type, count in cnt.items():
            metric_name = '.'.join([prefix, task_type])
            self.gauge(metric_name, count, tags=metric_tags)

    def _convert_to_per_sec(self, value, granularity):
        if value is None:
            return
        
        try:
            value = int(value)
        except ValueError:
            self.log.debug('Value not an int: %s', value)
            return

        if granularity == '15sec':
            return value / 15
        else:
            self.log.info('Unknown granularity: %s', granularity)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        path = sys.argv[1]
    else:
        print "Usage: python cloudant.py <path_to_config>"
    check, instances = CloudantCheck.from_yaml(path)
    for instance in instances:
        print "\nRunning the check against cluster: %s" % (instance['cluster'])
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())

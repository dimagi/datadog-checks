import requests

from checks import AgentCheck

SWIFT_AUTH_URL_KEY = 'swift_auth_url'
SWIFT_USERNAME_KEY = 'swift_username'
SWIFT_PASSWORD_KEY = 'swift_password'


class SwiftClient(object):
    def __init__(self, auth_url, username, password):
        self.auth_url = auth_url
        self.username = username
        self.password = password

        self.timeout = 30

        self.auth_token = None
        self.storage_url = None

    def authenticate(self):
        if not self.auth_token:
            resp = requests.get(self.auth_url, headers={
                'X-Auth-User': self.username,
                'X-Auth-Key': self.password
            })
            resp.raise_for_status()
            self.storage_url = resp.headers['X-Storage-Url']
            self.auth_token = resp.headers['X-Auth-Token']

    def _auth_headers(self):
        return {
            'X-Auth-Token': self.auth_token
        }

    def reset(self):
        self.auth_token = None

    def account_info(self):
        resp = requests.get(self.storage_url, headers=self._auth_headers(), timeout=self.timeout)
        resp.raise_for_status()
        return {
            'quota_bytes': int(resp.headers['X-Account-Meta-Quota-Bytes']),
            'used_bytes': int(resp.headers['X-Account-Bytes-Used']),
            'object_count': int(resp.headers['X-Account-Object-Count']),
            'container_count': int(resp.headers['X-Account-Container-Count']),
        }


class SwiftCheck(AgentCheck):
    """Extracts stats from OpenStack Swift"""
    SERVICE_CHECK_NAME = 'swift.can_connect'
    SOURCE_TYPE_NAME = 'swift'
    clients = {}

    @staticmethod
    def _validate_instance(instance):
        for key in [SWIFT_AUTH_URL_KEY, SWIFT_USERNAME_KEY, SWIFT_PASSWORD_KEY]:
            if key not in instance:
                raise Exception("A {} must be specified".format(key))

    def _get_client(self, instance):
        auth_url = instance[SWIFT_AUTH_URL_KEY]
        key = (auth_url, instance[SWIFT_USERNAME_KEY], instance[SWIFT_PASSWORD_KEY])
        if key not in self.clients:
            self.clients[key] = SwiftClient(
                auth_url, instance[SWIFT_USERNAME_KEY], instance[SWIFT_PASSWORD_KEY]
            )
        return self.clients[key]

    def check(self, instance):
        SwiftCheck._validate_instance(instance)

        tags = instance.get('tags', [])

        account_info = self.get_account_info(instance, tags)
        bytes_used = account_info['used_bytes']
        bytes_quota = account_info['quota_bytes']
        self.gauge('{}.object_count'.format(self.SOURCE_TYPE_NAME), account_info['object_count'], tags=tags)
        self.gauge('{}.container_count'.format(self.SOURCE_TYPE_NAME), account_info['container_count'], tags=tags)
        self.gauge('{}.used_bytes'.format(self.SOURCE_TYPE_NAME), bytes_used, tags=tags)
        self.gauge('{}.quota_bytes'.format(self.SOURCE_TYPE_NAME), bytes_quota, tags=tags)
        self.gauge('{}.quota_used_pct'.format(self.SOURCE_TYPE_NAME), bytes_used // bytes_quota, tags=tags)

    def get_account_info(self, instance, tags):
        url = instance[SWIFT_AUTH_URL_KEY]

        client = self._get_client(instance)
        try:
            client.authenticate()
            account_info = client.account_info()
        except requests.exceptions.Timeout as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                tags=tags, message="Request timeout: {0}, {1}".format(url, e))
            raise
        except requests.exceptions.HTTPError as e:
            client.reset()
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                tags=tags, message=str(e.message))
            raise
        except Exception as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                tags=tags, message=str(e))
            raise

        self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK,
            tags=tags,
            message='Connection to %s was successful' % url)
        return account_info

import psycopg2 as pg
import psycopg2.extras as pgextras
from collections import defaultdict
from six.moves.urllib.parse import urlparse

from checks import AgentCheck


class ShouldRestartException(Exception):
    pass


class PgBouncerCustom(AgentCheck):
    # Adapted from https://github.com/DataDog/integrations-core/blob/master/pgbouncer/datadog_checks/pgbouncer/pgbouncer.py
    DB_NAME = 'pgbouncer'
    SERVICE_CHECK_NAME = 'pgbouncer.can_connect'
    QUERY = 'SHOW CLIENTS'

    def _get_service_checks_tags(self, host, port, database_url, tags=None):
        if tags is None:
            tags = []

        if database_url:
            parsed_url = urlparse(database_url)
            host = parsed_url.hostname
            port = parsed_url.port

        service_checks_tags = [
            "host:%s" % host,
            "port:%s" % port,
            "db:%s" % self.DB_NAME
        ]
        service_checks_tags.extend(tags)
        service_checks_tags = list(set(service_checks_tags))

        return service_checks_tags

    def _collect_stats(self, db, instance_tags):
        try:
            with db.cursor(cursor_factory=pgextras.DictCursor) as cursor:
                try:
                    self.log.debug("Running query: %s", self.QUERY)
                    cursor.execute(self.QUERY)
                    rows = cursor.fetchall()
                except pg.Error:
                    self.log.exception("Not all metrics may be available")
                else:
                    count_by_db_and_host = defaultdict(lambda: defaultdict(int))
                    for row in rows:
                        self.log.debug("Processing row: %r", row)
                        db_name = row[2]
                        client_addr = row[4]
                        count_by_db_and_host[client_addr][db_name] += 1
                    for client, counts_by_host in count_by_db_and_host.iteritems():
                        for db_name, count in counts_by_host.iteritems():
                            tags = {'client_addr': client, 'db_name': db_name}
                            tags.extend(instance_tags)
                            self.gauge(
                                'pgbouncer_custom.clients.count',
                                count,
                                tags=tags
                            )
                    if not rows:
                        self.log.warning("No results were found for query: %s", self.QUERY)
        except pg.Error:
            self.log.exception("Connection error")
            raise ShouldRestartException

    def _get_connect_kwargs(self, host, port, user, password, database_url):
        """
        Get the params to pass to psycopg2.connect() based on passed-in vals
        from yaml settings file
        """
        if database_url:
            return {'dsn': database_url}

        if host in ('localhost', '127.0.0.1') and password == '':
            return {  # Use ident method
                'dsn': "user={} dbname={}".format(user, self.DB_NAME)
            }

        if port:
            return {'host': host, 'user': user, 'password': password,
                    'database': self.DB_NAME, 'port': port}

        return {'host': host, 'user': user, 'password': password,
                'database': self.DB_NAME}

    def _get_connection(self, key, host='', port='', user='',
                        password='', database_url='', tags=None, use_cached=True):
        if key in self.dbs and use_cached:
            return self.dbs[key]
        try:
            connect_kwargs = self._get_connect_kwargs(
                host=host, port=port, user=user,
                password=password, database_url=database_url
            )
            connection = pg.connect(**connect_kwargs)
            connection.set_isolation_level(
                pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        except Exception:
            redacted_url = self._get_redacted_dsn(host, port, user, database_url)
            message = u'Cannot establish connection to {}'.format(redacted_url)
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL,
                               tags=self._get_service_checks_tags(host, port, database_url, tags),
                               message=message)
            raise

        self.dbs[key] = connection
        return connection

    def _get_redacted_dsn(self, host, port, user, database_url):
        if not database_url:
            return u'pgbouncer://%s:******@%s:%s/%s' % (user, host, port, self.DB_NAME)

        parsed_url = urlparse(database_url)
        if parsed_url.password:
            return database_url.replace(parsed_url.password, '******')
        return database_url

    def check(self, instance):
        host = instance.get('host', '')
        port = instance.get('port', '')
        user = instance.get('username', '')
        password = instance.get('password', '')
        tags = instance.get('tags', [])
        database_url = instance.get('database_url')

        if database_url:
            key = database_url
        else:
            key = '%s:%s' % (host, port)

        try:
            db = self._get_connection(key, host, port, user, password, tags=tags,
                                      database_url=database_url, use_cached=True)
            self._collect_stats(db, tags)
        except ShouldRestartException:
            self.log.info("Resetting the connection")
            db = self._get_connection(key, host, port, user, password, tags=tags,
                                      database_url=database_url, use_cached=False)
            self._collect_stats(db, tags)

        redacted_dsn = self._get_redacted_dsn(host, port, user, database_url)
        message = u'Established connection to {}'.format(redacted_dsn)
        self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK,
                           tags=self._get_service_checks_tags(host, port, database_url, tags),
                           message=message)

import psycopg2 as pg
import psycopg2.extras as pgextras
from collections import Counter
from six.moves.urllib.parse import urlparse

from checks import AgentCheck


class ShouldRestartException(Exception):
    pass


class PgBouncerCustom(AgentCheck):
    # Adapted from https://github.com/DataDog/integrations-core/blob/master/pgbouncer/datadog_checks/pgbouncer/pgbouncer.py
    DB_NAME = 'pgbouncer'
    SERVICE_CHECK_NAME = 'pgbouncer.can_connect'
    QUERY = 'SHOW CLIENTS'

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
                    count_by_db_and_host = Counter([(row['addr'], row['database']) for row in rows])
                    for (addr, db), count in count_by_db_and_host:
                        tags = {'client_addr': addr, 'db_name': db}
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
        connect_kwargs = self._get_connect_kwargs(
            host=host, port=port, user=user,
            password=password, database_url=database_url
        )
        connection = pg.connect(**connect_kwargs)
        connection.set_isolation_level(
            pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        self.dbs[key] = connection
        return connection

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

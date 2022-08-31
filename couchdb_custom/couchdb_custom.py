import itertools
from collections import namedtuple
from urllib.parse import quote
from urllib.parse import urljoin

import requests
from datadog_checks.base import AgentCheck


class ShouldRestartException(Exception):
    pass


class SessionContext(namedtuple("SessionContext", "host port local_port session")):
    def request(self, path, host=None):
        return self._request(self.port, path, host)

    def local_request(self, path, host=None):
        return self._request(self.local_port, path, host)

    def _request(self, port, path, host):
        url = "http://{}:{}".format(host or self.host, port)
        response = self.session.get(url=urljoin(url, path))
        response.raise_for_status()
        return response.json()


class CouchDBCustom(AgentCheck):
    def check(self, instance):
        host = instance.get('host', '')
        port = instance.get('port', '')
        local_port = instance.get('local_port', '')
        user = instance.get('username', '')
        password = instance.get('password', '')
        instance_tags = instance.get('tags', [])

        with requests.Session() as session:
            session.auth = (user, password)
            context = SessionContext(host, port, local_port, session)
            node_hosts = _get_couch_nodes(context)
            shards = get_cluster_shard_details(context, node_hosts)
            for (db, shard_name), db_shards in itertools.groupby(shards, key=lambda s: (s["db_name"], s["shard_name"])):
                doc_counts = [shard["doc_count"] for shard in db_shards]
                diff = max(doc_counts) - min(doc_counts)
                self.gauge(
                    'couchdb.shard_doc_count_diff',
                    diff,
                    tags=instance_tags + ["database:{}".format(db), "shard:{}".format(shard_name)]
                )

            for shard in shards:
                tags = [
                    "node:{}".format(shard["node"]),
                    "database:{}".format(shard["db_name"]),
                    "shard:{}".format(shard["shard_name"]),
                ]
                self.gauge(
                    'couchdb.shard_doc_count',
                    shard["doc_count"],
                    tags=tags + instance_tags
                )
                self.gauge(
                    'couchdb.shard_doc_deletion_count',
                    shard["doc_del_count"],
                    tags=tags + instance_tags
                )


def _get_couch_nodes(context):
    membership = context.request('/_membership')
    return [node.split("@")[1] for node in membership["cluster_nodes"]]


def get_cluster_shard_details(context, nodes):
    return list(itertools.chain.from_iterable(
        _get_node_shard_details(context, node) for node in nodes
    ))


def _get_node_shard_details(context, host):
    shards = _get_node_shards(context, host)
    return [_get_shard_details(context, host, shard) for shard in shards]


def _get_node_shards(context, host):
    return [
        local_db for local_db in context.local_request("_all_dbs", host)
        if local_db.startswith("shards")
    ]


def _get_shard_details(context, host, shard_name):
    data = context.local_request(quote(shard_name, safe=""), host)
    shard, db = _get_shard_and_db(data["db_name"])
    data["node"] = host
    data["shard_name"] = shard
    data["db_name"] = db
    return data


def _get_shard_and_db(shard_name):
    """
    # shards/c0000000-dfffffff/commcarehq.1541009837
    return ("c0000000-dfffffff", "commcarehq")
    """
    split = shard_name.split("/")
    return split[1], split[-1].split(".")[0]

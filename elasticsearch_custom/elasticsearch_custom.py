import requests
from datadog_checks.base import AgentCheck


class ElasticsearchCustom(AgentCheck):
    def check(self, instance):
        url = instance.get('url', '')
        instance_tags = instance.get('tags', [])

        with requests.Session() as session:
            for index in _get_read_only_indices(session, url):
                self.gauge(
                    'elasticsearch.index.read_only_allow_delete',
                    1,
                    tags=instance_tags + ["index:{}".format(index)]
                )


def _get_read_only_indices(session, url):
    """Return the names of indices that have the read only block set.

    ES sets index.blocks.read_only_allow_delete to the string "true" on an index
    when disk usage crosses the flood-stage watermark; the block must be cleared
    manually. The _settings endpoint returns only indices that carry the setting.
    """
    endpoint = "{}/_all/_settings/index.blocks.read_only_allow_delete?flat_settings=true".format(url.rstrip("/"))
    response = session.get(endpoint)
    response.raise_for_status()
    settings = response.json()
    return [
        index
        for index, body in settings.items()
        if body.get("settings", {}).get("index.blocks.read_only_allow_delete") == "true"
    ]

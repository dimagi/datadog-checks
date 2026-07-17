# datadog-elasticsearch-custom
Custom metrics for Elasticsearch.

## Metric list

### elasticsearch.index.read_only_allow_delete
Gauge emitted with value `1` for each index that currently has
`index.blocks.read_only_allow_delete` set to `true`. Elasticsearch sets this
block on an index when disk usage crosses the flood-stage watermark
(`cluster.routing.allocation.disk.watermark.flood_stage`, default 95%). The block
is **not** cleared automatically when disk frees up. It must be reset manually,
and until then the index rejects writes.

No datapoint is emitted for healthy indices, so absence of the metric means no
index is blocked.

### Tags
* index: name of the affected Elasticsearch index

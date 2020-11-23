# datadog-couchdb-custom
Custom metrics for CouchDB.

## Metric list

### couchdb.shard_doc_count
Count of docs per shard / database / host combination.

### couchdb.shard_doc_deletion_count
Count of doc deletions per shard / database / host combination.

### Tags
* node: node name / IP address
* database: couchdb database name
* shard: shard name e.g. "c0000000-dfffffff"

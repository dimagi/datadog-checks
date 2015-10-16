# datadog-cloudant
Datadog check for cloudant.

See https://docs.cloudant.com/monitoring.html

## Metric list

### Requests per second by status code
- cloudant.http_status_code.2xx
- cloudant.http_status_code.3xx
- cloudant.http_status_code.4xx
- cloudant.http_status_code.5xx

### Requests per second by HTTP method
- cloudant.http_method.get
- cloudant.http_method.post
- cloudant.http_method.put
- cloudant.http_method.delete
- cloudant.http_method.copy
- cloudant.http_method.head

### Doc reads / writes per second
- cloudant.doc_writes
- cloudant.doc_reads

### Disk use (bytes)
- cloudant.disk_use.used
- cloudant.disk_use.free

### Documents processed by a map function, per second
- cloudant.map_doc

### key:value emits per second.
- cloudant.kv_emits

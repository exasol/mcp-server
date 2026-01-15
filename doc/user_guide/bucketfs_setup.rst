BucketFS Access Setup
=====================

Exasol On-Prem has a separate authentication for BucketFS. It is password-based,
with the username being different from the database username. This presents certain
limitations for the MCP Server running in a multi-user mode. Currently, the MCP Server
can only be configured to access the BucketFS under a single user account. Consequently,
only one BucketFS service and one bucket can be made accessible.

On-Prem configuration
---------------------

+-----------------------+-------------------------------------------------------------+
| Variable Name         | Description                                                 |
+=======================+=============================================================+
| EXA_BUCKETFS_URL      | BucketFS URL, for example "https://demodb.exasol.com:2202". |
+-----------------------+-------------------------------------------------------------+
| EXA_BUCKETFS_SERVICE  | Optional name of the BucketFS service,                      |
|                       | not required in most cases.                                  |
+-----------------------+-------------------------------------------------------------+
| EXA_BUCKETFS_BUCKET   | Optional name of the bucket, defaults to "default".         |
+-----------------------+-------------------------------------------------------------+
| EXA_BUCKETFS_USER     | BucketFS username.                                         |
+-----------------------+-------------------------------------------------------------+
| EXA_BUCKETFS_PASSWORD | Password for the selected bucket.                           |
+-----------------------+-------------------------------------------------------------+
| EXA_BUCKETFS_PATH     | Optional path in the bucket to be used as a root,           |
|                       | defaults to the bucket root.                                |
+-----------------------+-------------------------------------------------------------+

SaaS configuration
------------------

In the case of a SaaS backend, the BucketFS shares the authentication with the database.
Currently, there is only one BucketFS service and only one bucket. Therefore, no extra
configuration is required. The only optional configuration element that could be used
is `EXA_BUCKETFS_PATH`, which has the same meaning as in the On-Prem case.

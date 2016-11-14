# log4j-hbase

Writes log messages to an HBase table. The table must already exist with the families referenced by ColumnValues.
The only supported layout for ColumnValues is org.apache.log4j.PatternLayout which is hardcoded.

##Usage:
```
log4j.appender.HBase=com.app.HBaseAppender
log4j.appender.HBase.TableName=Logger
log4j.appender.HBase.BufferSize=500
log4j.appender.HBase.ColumnValues=d:log_level=%p, d:created=%d{yyyy-MM-dd HH:mm:ss}, d:class=%C, d:method=%M, d:line_number=%L, d:message=%m
```
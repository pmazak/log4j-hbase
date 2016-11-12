/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Writes log messages to an HBase table. The table must already exist with the families referenced by ColumnValues.
 * The only supported layout for ColumnValues is org.apache.log4j.PatternLayout which is hardcoded.
 * Usage:
 *  log4j.appender.HBase=estalea.ir.eaa.utils.HBaseAppender
 *  log4j.appender.HBase.TableName=Logger
 *  log4j.appender.HBase.BufferSize=500
 *  log4j.appender.HBase.ColumnValues=d:log_level=%p, d:created=%d{yyyy-MM-dd HH:mm:ss}, d:class=%C, d:method=%M, d:line_number=%L, d:message=%m
 */
public class HBaseAppender extends AppenderSkeleton implements Appender {

    // Configurable properties
    private String tableName;
    private Integer bufferSize;
    private String columnValues;

    // Internal fields
    private static final String INTERNAL_DELIMITER = "|&|";
    private static final String INTERNAL_DELIMITER_REGEX = "\\|&\\|";
    private HTable htable;
    private List<byte[]> families;
    private List<byte[]> columns;
    private String layoutValues;
    private Random random = new Random();
    private ByteBuffer bit128 = ByteBuffer.allocate(16);
    private ArrayList<LoggingEvent> buffer = new ArrayList<>();

    public HBaseAppender() {
    }

    protected void setHtable(HTable htable) {
        this.htable = htable;
    }

    protected HTable getHtable() {
        return htable;
    }

    /**
     * Public setter so property can be configured in log4j.properties
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
        Configuration conf = HBaseConfiguration.create(new Configuration());
        try {
            this.htable = new HTable(conf, tableName);
        } catch (IOException e) {
            errorHandler.error("Error opening HBase table", e, ErrorCode.GENERIC_FAILURE);
        }
    }

    public String getTableName() {
        return tableName;
    }
    
    /**
     * Public setter so property can be configured in log4j.properties
     */
    public void setBufferSize(Integer bufferSize) {
        this.bufferSize = bufferSize;
    }

    public Integer getBufferSize() {
        return bufferSize;
    }

    /**
     * Public setter so property can be configured in log4j.properties
     * @param columnValues - Comma-delimited log4j pattern of family:column=value pairs. For example:
     *   d:log_level=%p, d:created=%d{yyyy-MM-dd HH:mm:ss}, d:class=%C, d:method=%M, d:line_number=%L, d:message=%m
     */
    public void setColumnValues(String columnValues) {
        this.columnValues = columnValues;
        setLayoutValues();
    }

    public String getColumnValues() {
        return columnValues;
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    private void setLayoutValues() {
        if (this.columnValues == null)
            return;
        this.families = new ArrayList<>();
        this.columns = new ArrayList<>();
        StringBuilder normalizedValues = new StringBuilder();
        for (String famColVal : this.columnValues.split(",")) {
            famColVal = famColVal.trim();
            int firstColonPos = famColVal.indexOf(":");
            int firstEqualPos = famColVal.indexOf("=");
            families.add(Bytes.toBytes(famColVal.substring(0, firstColonPos).trim()));
            columns.add(Bytes.toBytes(famColVal.substring(firstColonPos + 1, firstEqualPos).trim()));
            if (normalizedValues.length() > 0) {
                normalizedValues.append(INTERNAL_DELIMITER);
            }
            normalizedValues.append(famColVal.substring(firstEqualPos + 1).trim());
        }
        this.layoutValues = normalizedValues.toString();
        if (getLayout() == null) {
            this.setLayout(new PatternLayout(this.layoutValues));
        }
        else {
            ((PatternLayout)getLayout()).setConversionPattern(this.layoutValues);
        }
    }

    public String getLayoutValues() {
        return this.layoutValues;
    }

    private String[] getLayoutValues(LoggingEvent event) {
        return getLayout().format(event).split(INTERNAL_DELIMITER_REGEX);
    }

    @Override
    protected void append(LoggingEvent event) {
        event.getNDC();
        event.getThreadName();
        // Get a copy of this thread's MDC.
        event.getMDCCopy();
        event.getLocationInformation();
        event.getRenderedMessage();
        event.getThrowableStrRep();
        buffer.add(event);
        if (bufferSize == null || buffer.size() >= bufferSize)
            flushBuffer();
    }

    @Override
    public void close() {
        flushBuffer();
        try {
            if (htable != null) {
                htable.close();
            }
        } catch (IOException e) {
            errorHandler.error("Error closing HBase table", e, ErrorCode.GENERIC_FAILURE);
        }
        this.closed = true;
    }

    /**
     * loops through the buffer of LoggingEvents, gets a
     * hbase columns and layoutValues to do a batch Put.
     * Errors are sent to the errorHandler.
     */
    public void flushBuffer() {
        List<Row> batchedPuts = new ArrayList<>();
        for (LoggingEvent loggingEvent : buffer) {
            try {
                String[] columnValues = getLayoutValues(loggingEvent);
                Put putRequest = new Put(Bytes.toBytes(createUniqueId()));
                for (int i = 0; i < columnValues.length; i++) {
                    putRequest.addColumn(families.get(i), columns.get(i), Bytes.toBytes(columnValues[i]));
                }
                putRequest.setDurability(Durability.ASYNC_WAL);
                batchedPuts.add(putRequest);
            } catch (Exception e) {
                errorHandler.error("Failed to build HBase Put request", e, ErrorCode.FLUSH_FAILURE);
            }
        }
        try {
            if (htable != null) {
                HTableUtil.bucketRsBatch(htable, batchedPuts);
                // HTableUtil is deprecated but it's faster than:
                // htable.batch(batchedPuts, new Object[batchedPuts.size()]);
            }
        }
        catch (Exception e) {
            errorHandler.error("Failed to execute HBase Put", e, ErrorCode.FLUSH_FAILURE);
        } finally {
            buffer.clear();
        }
    }

    private String createUniqueId() {
        bit128.clear();
        bit128.putInt(random.nextInt());
        bit128.putLong(System.nanoTime());
        bit128.putInt(random.nextInt());
        return MD5Hash.getMD5AsHex(bit128.array());
    }
}
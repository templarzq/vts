package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class MilvusBufferReader {
    private final Collector<SeaTunnelRow> output;
    private final MilvusSourceConverter milvusSourceConverter;
    private final MilvusClientV2 milvusClient;
    private final String collectionName;
    private final String partitionName;
    private final Long offset;
    private final Long limit;
    private final TableSchema tableSchema;
    private final MilvusSourceSplit split;
    private final CountDownLatch completionSignal = new CountDownLatch(1);
    private long rateLimitRetryIntervalMs = 30000;
    /** Configurable collection load retry params — settable before {@link #pollData}. */
    private int loadMaxRetries = 60;
    private long loadRetryDelayMs = 5000;
    /** Optional rate limiter callback — called with batch size before reading each batch. */
    private final java.util.function.IntConsumer rateLimiter;

    public MilvusBufferReader(MilvusSourceSplit split, Collector<SeaTunnelRow> output,
                              MilvusClientV2 client, TableSchema tableSchema) {
        this(split, output, client, tableSchema, null);
    }

    public MilvusBufferReader(MilvusSourceSplit split, Collector<SeaTunnelRow> output,
                              MilvusClientV2 client, TableSchema tableSchema,
                              java.util.function.IntConsumer rateLimiter) {
        this.output = output;
        this.milvusClient = client;
        this.tableSchema = tableSchema;
        this.split = split;
        this.rateLimiter = rateLimiter;
        this.milvusSourceConverter = new MilvusSourceConverter(tableSchema);
        this.collectionName = split.getTablePath().getTableName();
        this.partitionName = split.getPartitionName();
        this.offset = split.getOffset();
        this.limit = split.getLimit();
    }

    /** Configure collection load retry behavior. Call before {@link #pollData}. */
    public void setLoadRetryConfig(int maxRetries, long delayMs) {
        this.loadMaxRetries = maxRetries;
        this.loadRetryDelayMs = delayMs;
    }

    /**
     * Ensure the collection is loaded. If not, try to load it and wait for it
     * to become ready (with retries). Throws if loading fails after max retries.
     */
    private void ensureCollectionLoaded(String collectionName) {
        final int maxRetries = loadMaxRetries;
        final long waitMs = loadRetryDelayMs;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            boolean loaded;
            try {
                loaded = milvusClient.getLoadState(
                        GetLoadStateReq.builder().collectionName(collectionName).build());
            } catch (Exception e) {
                log.warn("Failed to check load state for collection '{}' (attempt {}/{}): {}",
                        collectionName, attempt, maxRetries, e.getMessage());
                loaded = false;
            }

            if (loaded) {
                return;
            }

            if (attempt == 1) {
                log.warn("Collection '{}' is not loaded, attempting to load...", collectionName);
                try {
                    milvusClient.loadCollection(
                            LoadCollectionReq.builder().collectionName(collectionName).build());
                    log.info("Load request sent for collection '{}'", collectionName);
                } catch (Exception e) {
                    log.warn("Load collection request failed for '{}': {}. Will retry...",
                            collectionName, e.getMessage());
                }
            }

            if (attempt < maxRetries) {
                try {
                    Thread.sleep(waitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MilvusConnectorException(
                            MilvusConnectionErrorCode.COLLECTION_NOT_LOADED,
                            "Interrupted while waiting for collection '" + collectionName + "' to load");
                }
            }
        }

        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.COLLECTION_NOT_LOADED,
                "Collection '" + collectionName + "' failed to load after " + maxRetries + " retries");
    }

    public void pollData(Integer batchSize) {
        log.info("Starting to read data from Milvus, table schema: {}", tableSchema.toString());
        ensureCollectionLoaded(collectionName);

        log.info("Collection '{}' is loaded. Starting query execution...", collectionName);
        // query iterate data in background
        try {
            queryIteratorData(collectionName, partitionName, batchSize, offset, limit);
        } catch (Exception e) {
            log.error("Error in queryIteratorData task: ", e);
            throw new MilvusConnectorException(MilvusConnectionErrorCode.READ_DATA_FAIL, e);
        }
    }

    private void queryIteratorData(String collectionName, String partitionName, long batchSize, Long offset, Long limit) throws InterruptedException {
        log.info("Querying data from Milvus: collection={}, batchSize={}, offset={}, limit={}", collectionName, batchSize, offset, limit);

        QueryIteratorReq queryIteratorReq = QueryIteratorReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .batchSize(batchSize)
                .build();

        if (StringUtils.isNotEmpty(partitionName)) {
            queryIteratorReq.setPartitionNames(Collections.singletonList(partitionName));
        }
        if (offset != null){
            queryIteratorReq.setOffset(offset);
        }
        if (limit != null){
            queryIteratorReq.setLimit(limit);
        }

        QueryIterator iterator = milvusClient.queryIterator(queryIteratorReq);

        int maxFailRetry = 3;

        while (true) {
            try {
                List<QueryResultsWrapper.RowRecord> next = iterator.next();
                if (next == null || next.isEmpty()) {
                    log.info("No more records in iterator");
                    completionSignal.countDown();
                    break;
                } else {
                    // Proactive rate limiting — throttle before processing the batch
                    if (rateLimiter != null) {
                        rateLimiter.accept(next.size());
                    }
                    long currentOffset = (offset != null) ? offset : 0;
                    for (QueryResultsWrapper.RowRecord record : next) {
                        SeaTunnelRow seaTunnelRow = milvusSourceConverter.convertToSeaTunnelRow(record, tableSchema, collectionName, partitionName);
                        validateVectorFields(seaTunnelRow, collectionName, partitionName, currentOffset);
                        seaTunnelRow.setTableId(split.getTablePath().toString());
                        output.collect(seaTunnelRow);
                        currentOffset++;
                    }
                    // Reset retry counter on successful batch
                    maxFailRetry = 3;
                }
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("rate limit exceeded")) {
                    maxFailRetry--;
                    if (maxFailRetry <= 0) {
                        throw new MilvusConnectorException(MilvusConnectionErrorCode.READ_DATA_FAIL,
                                "Rate limit retries exhausted for collection: " + collectionName, e);
                    }
                    // Exponential backoff: 1s, 2s, 4s... capped at rateLimitRetryIntervalMs
                    long backoff = Math.min(1000L << (3 - maxFailRetry), rateLimitRetryIntervalMs);
                    log.warn("Rate limit exceeded for collection '{}'. Retrying in {}ms (retries left: {})",
                            collectionName, backoff, maxFailRetry);
                    Thread.sleep(backoff);
                } else {
                    log.error("Query failed. Batch size: {}. Aborting...", batchSize, e);
                    throw new RuntimeException("Query failed", e);
                }
            }
        }

        log.info("Query execution completed for collection '{}'", collectionName);
    }

    /**
     * Validate and sanitize vector field values in a SeaTunnelRow.
     * Replaces NaN → 0.0f, +Infinity → Float.MAX_VALUE, -Infinity → -Float.MAX_VALUE
     * in ByteBuffer-backed float vectors, which are the canonical vector format from Milvus.
     * pgvector rejects NaN and Infinity, so we must sanitize at the source.
     */
    private void validateVectorFields(SeaTunnelRow row, String collection, String partition,
                                       long rowOffset) {
        Object[] fields = row.getFields();
        if (fields == null) {
            return;
        }
        for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
            sanitizeVectorField(fields, fieldIdx, collection, partition, rowOffset);
        }
    }

    /**
     * Check a single field for NaN/Infinity float values and sanitize in place.
     * Handles ByteBuffer (Milvus native vector format), List&lt;Float&gt;, and float[].
     */
    @SuppressWarnings("unchecked")
    private void sanitizeVectorField(Object[] fields, int fieldIdx, String collection,
                                      String partition, long rowOffset) {
        Object field = fields[fieldIdx];
        if (field instanceof ByteBuffer) {
            // Milvus native vector format: little-endian float ByteBuffer.
            // Use capacity() instead of remaining() because the buffer position
            // may have been advanced by downstream converters before reaching us.
            ByteBuffer buf = (ByteBuffer) field;
            int floatCount = buf.capacity() / Float.BYTES;
            boolean modified = false;
            // Read/write at absolute positions via a duplicate so we don't
            // disturb the original buffer's position/limit.
            ByteBuffer dup = buf.duplicate();
            dup.order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < floatCount; i++) {
                float f = dup.getFloat(i * Float.BYTES);
                if (Float.isNaN(f)) {
                    dup.putFloat(i * Float.BYTES, 0.0f);
                    modified = true;
                    log.warn("Float.NaN replaced with 0.0f in vector field[{}], dim[{}], "
                            + "row offset={} (collection={}, partition={})",
                            fieldIdx, i, rowOffset, collection, partition);
                } else if (Float.isInfinite(f)) {
                    float replacement = f > 0 ? Float.MAX_VALUE : -Float.MAX_VALUE;
                    dup.putFloat(i * Float.BYTES, replacement);
                    modified = true;
                    log.warn("Float.Infinity ({}) replaced with {} in vector field[{}], dim[{}], "
                            + "row offset={} (collection={}, partition={})",
                            f, replacement, fieldIdx, i, rowOffset, collection, partition);
                }
            }
            if (modified) {
                // Update the original buffer with sanitized data
                buf.rewind();
                dup.rewind();
                buf.put(dup);
                buf.rewind();
            }
        } else if (field instanceof List) {
            List<?> list = (List<?>) field;
            for (Object item : list) {
                if (item instanceof Float) {
                    float f = (Float) item;
                    if (Float.isNaN(f) || Float.isInfinite(f)) {
                        log.warn("Invalid float value ({}) in vector field[{}] at row offset {} "
                                        + "(collection={}, partition={})",
                                f, fieldIdx, rowOffset, collection, partition);
                        break;
                    }
                }
            }
        } else if (field instanceof float[]) {
            float[] arr = (float[]) field;
            for (int i = 0; i < arr.length; i++) {
                float f = arr[i];
                if (Float.isNaN(f)) {
                    arr[i] = 0.0f;
                    log.warn("Float.NaN replaced with 0.0f in vector field[{}], dim[{}], "
                            + "row offset={} (collection={}, partition={})",
                            fieldIdx, i, rowOffset, collection, partition);
                } else if (Float.isInfinite(f)) {
                    float replacement = f > 0 ? Float.MAX_VALUE : -Float.MAX_VALUE;
                    arr[i] = replacement;
                    log.warn("Float.Infinity ({}) replaced with {} in vector field[{}], dim[{}], "
                            + "row offset={} (collection={}, partition={})",
                            f, replacement, fieldIdx, i, rowOffset, collection, partition);
                }
            }
        }
    }
}
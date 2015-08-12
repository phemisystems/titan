package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager.ScanConfig;

/**
 * KCVS implementation backed by Apache Accumulo. Delegates to the AccumuloStoreManager for write operations.
 * A "store" maps to a single column family and locality group in Accumulo, so all operations on this store are
 * performed on the same column family.
 */
public class AccumuloKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloKeyColumnValueStore.class);

    private final String storeName;
    private final AccumuloStoreManager storeManager;
    private final Text colFam;
    private final AccumuloGetter entryGetter;

    public AccumuloKeyColumnValueStore(AccumuloStoreManager storeManager, String storeName) {
        this.storeManager = storeManager;
        this.storeName = storeName;
        this.colFam = new Text(storeName.getBytes(UTF_8));
        this.entryGetter = new AccumuloGetter(storeManager.getMetaDataSchema(storeName));
    }

    /**
     * A KeySliceQuery appears to be a range of column qualifiers for a single logical row. Looking at the HBase
     * implementation, it seems to be only interested in the latest version of the matching keys.
     * @param query Query to get results for
     * @param txh   ignored
     * @return A list of all matching entries. Note that the full response will be held in memory.
     * @throws BackendException if the query could not be performed.
     */
    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        logger.trace("In getSlice(KeySliceQuery), query == {}", query);
        ScanConfig cfg = new ScanConfig(colFam).onlyLatestVersion();
        BatchScanner scanner = storeManager.newBatchScanner(cfg);
        try {
            Text row = new Text(query.getKey().as(StaticBuffer.ARRAY_FACTORY));
            Text cqStart = new Text(query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY));
            Text cqEnd = new Text(query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY));
            Range r;
            try {
                r = new Range(new Key(row, colFam, cqStart), true, new Key(row, colFam, cqEnd), false);
            } catch (IllegalArgumentException iae) {
                throw new TemporaryBackendException("Invalid slice; " + cqStart + "..." + cqEnd, iae);
            }
            int numReceived = 0;
            scanner.setRanges(Collections.singletonList(r));
            Iterator<Map.Entry<Key, Value>> kvPairs = scanner.iterator();
            List<Entry> entries = new LinkedList<Entry>();
            while (kvPairs.hasNext() && numReceived < query.getLimit()) {
                Map.Entry<Key, Value> kvPair = kvPairs.next();
                entries.add(StaticArrayEntry.ofBytes(kvPair, entryGetter));
                numReceived++;
            }
            return StaticArrayEntryList.of(entries);
        } finally {
            scanner.close();
        }
    }

    /**
     * A slice query is a range of column qualifiers for a selection of logical rows.
     * @param keys  List of keys
     * @param query Slicequery specifying matching entries
     * @param txh   Transaction
     * @return A list of all matching entries. Note that the entire resultset will be held in memory.
     * @throws BackendException
     */
    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        ScanConfig cfg = new ScanConfig(colFam)
                .onlyLatestVersion()
                .setCqSlice(query)
                .enableWholeRow();
        BatchScanner scanner = storeManager.newBatchScanner(cfg);
        List<Range> ranges = new LinkedList<Range>();
        for (StaticBuffer key: keys) {
            ranges.add(Range.exact(new Text(key.as(StaticBuffer.ARRAY_FACTORY)), colFam));
        }
        Map<StaticBuffer, EntryList> result = new HashMap<StaticBuffer, EntryList>();
        try {
            scanner.setRanges(ranges);
            for (Map.Entry<Key,Value> kvPair: scanner) {
                byte[] rowId = kvPair.getKey().getRowData().toArray();
                SortedMap<Key, Value> pairMap;
                try {
                    pairMap = WholeRowIterator.decodeRow(kvPair.getKey(), kvPair.getValue());
                } catch (IOException ioe) {
                    logger.warn("Error decoding row " + new String(rowId));
                    continue;
                }
                result.put(StaticArrayBuffer.of(rowId),
                        StaticArrayEntryList.of(getEntryList(rowId, pairMap, query.getLimit())));
            }
            return result;
        } finally {
            scanner.close();
        }
    }

    /**
     * Writes the provided additions and deletions to Accumulo.
     * @param key       the key (row id) under which the columns in {@code additions} and
     *                  {@code deletions} will be written
     * @param additions the list of Entry instances representing column-value pairs to
     *                  create under {@code key}, or null to add no column-value pairs
     * @param deletions the list of columns to delete from {@code key}, or null to
     *                  delete no columns
     * @param txh       ignored.
     * @throws BackendException
     */
    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        if (logger.isTraceEnabled()) {
            logger.trace("In mutate, writing {} additions, {} deletions", additions.size(), deletions.size());
        }
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        storeManager.mutateMany(ImmutableMap.of(storeName, mutations), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("AccumuloKeyColumnValueStore doesn't support locks");
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        logger.trace("In getKeys(KeyRangeQuery), query == {}", query);
        return doKeyIteratorQuery(query.getKeyStart().as(StaticBuffer.ARRAY_FACTORY),
                query.getKeyEnd().as(StaticBuffer.ARRAY_FACTORY), query);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        logger.trace("In getKeys(SliceQuery), query == {}", query);
        return doKeyIteratorQuery(null, null, query);
    }

    public KeyIterator doKeyIteratorQuery(byte[] start, byte[] end, SliceQuery columnSlice) throws BackendException {
        ScanConfig cfg = new ScanConfig(colFam).enableWholeRow()
                .onlyLatestVersion()
                .setCqSlice(columnSlice);
        BatchScanner bs = storeManager.newBatchScanner(cfg);
        Text startRow = start == null ? null : new Text(start);
        Text endRow = end == null ? null : new Text(end);
        bs.setRanges(Collections.singletonList(new Range(startRow, true, endRow, false)));
        return new RowIterator(bs, columnSlice.getLimit());
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public void close() throws BackendException {
        // Do nuffin'
    }

    private EntryList getEntryList(byte[] rowId, SortedMap<Key,Value> pairs, int limit) {
        Key startKey = new Key(new Text(rowId), colFam);
        Key afterLastKey = startKey.followingKey(PartialKey.ROW_COLFAM);
        Iterator<Map.Entry<Key,Value>> pairIter = pairs.subMap(startKey, afterLastKey).entrySet().iterator();
        List<Entry> entries = new LinkedList<Entry>();
        int numEntries = 0;
        while (pairIter.hasNext() && numEntries < limit) {
            numEntries++;
            Map.Entry<Key,Value> pair = pairIter.next();
            entries.add(StaticArrayEntry.ofBytes(pair, entryGetter));
        }
        return StaticArrayEntryList.of(entries);
    }

    private static class AccumuloGetter implements StaticArrayEntry.GetColVal<Map.Entry<Key, Value>, byte[]> {
        private final EntryMetaData[] schema;

        private AccumuloGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public byte[] getColumn(Map.Entry<Key, Value> element) {
            return element.getKey().getColumnQualifier().copyBytes();
        }

        @Override
        public byte[] getValue(Map.Entry<Key, Value> element) {
            return element.getValue().get();
        }

        @Override
        public EntryMetaData[] getMetaSchema(Map.Entry<Key, Value> element) {
            return schema;
        }

        @Override
        public Object getMetaData(Map.Entry<Key, Value> element, EntryMetaData meta) {
            switch(meta) {
                case TIMESTAMP:
                    return element.getKey().getTimestamp();
                case VISIBILITY:
                    return element.getKey().getColumnVisibility().toString();
                default:
                    throw new UnsupportedOperationException("Unsupported metadata: " + meta);
            }
        }
    }



    private class RowIterator implements KeyIterator {

        private final BatchScanner scanner;
        private final int limit;
        private final Iterator<Map.Entry<Key,Value>> scanIter;
        private boolean isClosed;
        private EntryList currentRowEntries;

        public RowIterator(BatchScanner scanner, int limit) {
            this.scanner = scanner;
            this.limit = limit;
            this.scanIter = scanner.iterator();
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();
            return new RecordIterator<Entry>() {
                private final Iterator<Entry> entries = currentRowEntries.iterator();

                @Override
                public void close() throws IOException {
                    isClosed = true;
                }

                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }

                @Override
                public Entry next() {
                    return entries.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Can't remove entries from Accumulo record iterator");
                }
            };
        }

        @Override
        public void close() throws IOException {
            try {
                scanner.close();
            } finally {
                isClosed = true;
            }
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return scanIter.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();
            Map.Entry<Key,Value> currentRow = scanIter.next();
            try {
                SortedMap<Key, Value> currentRowDecoded = WholeRowIterator.decodeRow(currentRow.getKey(), currentRow.getValue());
                currentRowEntries = getEntryList(currentRow.getKey().getRowData().toArray(),
                        currentRowDecoded, limit);
                return StaticArrayBuffer.of(currentRow.getKey().getRow().copyBytes());
            } catch (IOException ioe) {
                throw new RuntimeException("Error processing row " + currentRow.getKey(), ioe);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Can't remove items from Accumulo row iterator");
        }

        private void ensureOpen() {
            if (isClosed) {
                throw new IllegalStateException("Iterator has been closed.");
            }
        }
    }
}

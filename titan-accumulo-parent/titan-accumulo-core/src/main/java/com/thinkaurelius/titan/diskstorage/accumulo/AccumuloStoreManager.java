package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.CustomizeStoreKCVSManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import com.thinkaurelius.titan.util.system.NetworkUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AccumuloStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager,
        CustomizeStoreKCVSManager {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloStoreManager.class);

    private static final int MAX_PARTITION_ATTEMPTS = 100;
    private static final int AVG_PARTITION_RETRY_MS = 100;

    public static final ConfigNamespace ACCUMULO_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "accumulo", "Accumulo storage options");

    public static final ConfigOption<Boolean> SKIP_SCHEMA_CHECK =
            new ConfigOption<Boolean>(ACCUMULO_NS, "skip-schema-check",
                    "Assume that Titan's Accumulo table already exists. " +
                    "When this is true, Titan will not check for the existence of its table, " +
                    "nor will it attempt to create it under any circumstances.",
                    ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<String> ACCUMULO_TABLE =
            new ConfigOption<String>(ACCUMULO_NS, "table",
                    "The name of the table Titan will use. When " + ConfigElement.getPath(SKIP_SCHEMA_CHECK) +
                    " is false, Titan will automatically create this table if it does not already exist.",
                    ConfigOption.Type.LOCAL, "titan");

    private static final String CLIENT_CONF_FILE_DEFAULT = "-DEFAULT-";

    public static final ConfigOption<String> CLIENT_CONF_FILE =
            new ConfigOption<String>(ACCUMULO_NS, "client-conf-file",
                    "The path to Accumulo's client.conf file, containing the settings necessary to connect to the " +
                    "Accumulo cluster. If not set, will follow rules defined by Accumulo's ClientConfiguration.loadDefault",
                    ConfigOption.Type.LOCAL, CLIENT_CONF_FILE_DEFAULT);

    public static final ConfigOption<String> AUTHS =
            new ConfigOption<String>(ACCUMULO_NS, "scan-auths",
                    "Comma-separated list of authorizations to use when querying Accumulo. The authorizations, if " +
                    "specified, must be a subset of the user's authorizations.", ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<Integer> NUM_SCAN_THREADS =
            new ConfigOption<Integer>(ACCUMULO_NS, "num-scan-threads",
                    "Number of threads to use when creating a batch scanner to query Accumulo. Default is 5.",
                    ConfigOption.Type.LOCAL, 5);

    public static final ConfigOption<Long> TABLE_TTL =
            new ConfigOption<Long>(ACCUMULO_NS, "table-ttl",
                    "Time-to-live (millis) defined at the table level. Determines how long KV pairs may exist in the " +
                            "table before they're expunged. TTLs may be also be defined per-CF. These TTLs are " +
                            "enforced at scan time.", ConfigOption.Type.LOCAL, Long.MAX_VALUE);

    public static final ConfigNamespace ACCUMULO_CONFIGURATION_NAMESPACE =
            new ConfigNamespace(ACCUMULO_NS, "ext", "Overrides for client.conf options", true);

    private static final int PRIORITY_STORE_TTL = 20;
    private static final int PRIORITY_LATEST_VERSION = 25;
    private static final int PRIORITY_CQ_SLICE = 30;
    private static final int PRIORITY_WHOLE_ROW = 50;

    private static final IteratorSetting LATEST_VERSION = new IteratorSetting(PRIORITY_LATEST_VERSION, "LatestVersion",
            VersioningIterator.class);

    static {
        VersioningIterator.setMaxVersions(LATEST_VERSION, 1);
    }

    private static final ConcurrentHashMap<AccumuloStoreManager, Throwable> openManagers =
            new ConcurrentHashMap<AccumuloStoreManager, Throwable>();

    public static final int PORT_DEFAULT = 9160;

    private static final StaticBuffer FOUR_ZERO_BYTES = BufferUtil.zeroBuffer(4);

    private final ConcurrentHashMap<String, AccumuloKeyColumnValueStore> openStores;
    private final String tableName;
    private final boolean skipSchemaCheck;
    private final ClientConfiguration clientConf;
    private final Authorizations scanAuths;
    private final int numScanThreads;
    private final long tableTtl;
    private final Map<String, IteratorSetting> storeTtls;
    private final Instance accInstance;

    public AccumuloStoreManager(Configuration storageConfig) throws BackendException {
        super(storageConfig, PORT_DEFAULT);
        tableName = storageConfig.get(ACCUMULO_TABLE);
        skipSchemaCheck = storageConfig.get(SKIP_SCHEMA_CHECK);
        clientConf = loadClientConfig(storageConfig);
        accInstance = new ZooKeeperInstance(clientConf);
        scanAuths = new Authorizations(storageConfig.get(AUTHS).split(","));
        numScanThreads = storageConfig.get(NUM_SCAN_THREADS);
        tableTtl = storageConfig.get(TABLE_TTL);
        storeTtls = new HashMap<String,IteratorSetting>();
        if (!hasAuthentication()) {
            throw new PermanentBackendException("AccumuloStoreManager requires authentication, set " +
                    GraphDatabaseConfiguration.AUTH_USERNAME + " and " +
                    GraphDatabaseConfiguration.AUTH_PASSWORD);
        }
        checkConfigDeprecation(storageConfig);

        if (logger.isTraceEnabled()) {
            openManagers.put(this, new Throwable("Manager opened"));
            dumpOpenManagers();
        }
        openStores = new ConcurrentHashMap<String, AccumuloKeyColumnValueStore>();
    }

    private ClientConfiguration loadClientConfig(Configuration storageConfig) throws PermanentBackendException {
        ClientConfiguration clientConf;
        String clientConfFile = storageConfig.get(CLIENT_CONF_FILE);
        if (CLIENT_CONF_FILE_DEFAULT.equals(clientConfFile)) {
            clientConf = ClientConfiguration.loadDefault();
        } else {
            try {
                clientConf = new ClientConfiguration(clientConfFile);
            } catch (ConfigurationException ce) {
                throw new PermanentBackendException("Unable to load client configuration from " + clientConfFile, ce);
            }
        }
        // Overlay custom properties
        Map<String,Object> configSub = storageConfig.getSubset(ACCUMULO_CONFIGURATION_NAMESPACE);
        for (Map.Entry<String,Object> entry : configSub.entrySet()) {
            logger.info("Accumulo configuration: setting {}={}", entry.getKey(), entry.getValue());
            if (entry.getValue() != null) {
                clientConf.setProperty(entry.getKey(), entry.getValue().toString());
            }
        }
        // As in the HBase handler, special-case the storage hosts config
        if (storageConfig.has(GraphDatabaseConfiguration.STORAGE_HOSTS)) {
            String hostList = Joiner.on(",").join(storageConfig.get(GraphDatabaseConfiguration.STORAGE_HOSTS));
            clientConf.setProperty(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST, hostList);
        }
        if (logger.isDebugEnabled()) {
            logger.warn("Dumping Accumulo client configuration");
            logger.warn(clientConf.serialize());
            logger.warn("End of Accumulo client configuration");
        }
        return clientConf;
    }

    @Override
    public Deployment getDeployment() {
        List<KeyRange> local;
        try {
            local = getLocalKeyPartition();
            return null != local && !local.isEmpty() ? Deployment.LOCAL : Deployment.REMOTE;
        } catch (BackendException e) {
            // propagating StorageException might be a better approach
            throw new RuntimeException(e);
        }
    }

    @Override
    public KeyColumnValueStore openDatabase(String name) throws BackendException {
        return openDatabase(name, Integer.MAX_VALUE);
    }

    @Override
    public KeyColumnValueStore openDatabase(String longName, int ttlInSeconds) throws BackendException {
        AccumuloKeyColumnValueStore store = openStores.get(longName);
        TableOperations tableOps = newConnector().tableOperations();
        if (store == null) {
            AccumuloKeyColumnValueStore newStore = new AccumuloKeyColumnValueStore(this, longName);

            store = openStores.putIfAbsent(longName, newStore); // nothing bad happens if we lose to other thread

            if (store == null) {
                if (!skipSchemaCheck) {
                    try {
                        tableOps.create(tableName, new NewTableConfiguration().withoutDefaultIterators());
                        IteratorSetting ttlSetting = new IteratorSetting(20, "AgeOffFilter",
                                "org.apache.accumulo.core.iterators.user.AgeOffFilter");
                        ttlSetting.addOption("ttl", Long.toString(tableTtl));
                        tableOps.attachIterator(tableName, ttlSetting, EnumSet.allOf(IteratorUtil.IteratorScope.class));
                    } catch (AccumuloException ae) {
                        throw new PermanentBackendException("Error connecting to Accumulo", ae);
                    } catch (AccumuloSecurityException ase) {
                        throw new PermanentBackendException("Credentials failure creating table " + tableName, ase);
                    } catch (TableExistsException ignore) {
                        logger.info("Table {} exists.", tableName);
                    } catch (TableNotFoundException tnfe) {
                        logger.info("Unable to set TTL on table " + tableName, tnfe);
                    }
                }
                try {
                    synchronized (this) {
                        Map<String, Set<Text>> localityGroups = tableOps.getLocalityGroups(tableName);
                        Set<Text> storeNameLg = localityGroups.get(longName);
                        if (storeNameLg != null) {
                            if (storeNameLg.size() != 1 || !storeNameLg.contains(new Text(longName))) {
                                throw new PermanentBackendException("Locality group for " + longName + " already exists " +
                                        "but is incorrectly configured");
                            }
                        } else {
                            Map<String, Set<Text>> newLocalityGroups = new HashMap<String, Set<Text>>(localityGroups);
                            newLocalityGroups.put(longName, Collections.singleton(new Text(longName)));
                            tableOps.setLocalityGroups(tableName, newLocalityGroups);
                        }
                    }
                } catch (AccumuloException ae) {
                    throw new PermanentBackendException("Unable to update locality group " + longName + " on table " + tableName, ae);
                } catch (AccumuloSecurityException ase) {
                    throw new PermanentBackendException("Error updating locality group " + longName + " on table " + tableName, ase);
                } catch (TableNotFoundException tnfe) {
                    throw new PermanentBackendException("Could not set locality groups on table " + tableName, tnfe);
                }
                if (ttlInSeconds != Integer.MAX_VALUE) {
                    IteratorSetting ttlSetting = new IteratorSetting(PRIORITY_STORE_TTL, "TitanStoreTTL",
                            AgeOffFilter.class);
                    AgeOffFilter.setTTL(ttlSetting, (long)ttlInSeconds * 1000);
                    storeTtls.put(longName, ttlSetting);
                }
                store = newStore;
            }
        }

        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh)
            throws BackendException {
        BatchWriter writer = null;
        try {
            writer = newConnector().createBatchWriter(tableName, new BatchWriterConfig());
            mutateMany(writer, mutations, txh);
        } catch (TableNotFoundException tnfe) {
            throw new PermanentBackendException("Table " + tableName + " does not exist", tnfe);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (MutationsRejectedException mre) {
                    throw new TemporaryBackendException("Unable to write mutations to " + tableName, mre);
                }
            }
        }
    }

    public void mutateMany(final BatchWriter writer, Map<String, Map<StaticBuffer, KCVMutation>> mutations,
                           StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        Collection<Mutation> accumuloMuts = convertMutations(mutations, commitTime.getAdditionTime(times.getUnit()),
                commitTime.getDeletionTime(times.getUnit()));
        try {
            writer.addMutations(accumuloMuts);
            writer.flush();
        } catch (MutationsRejectedException mre) {
            throw new TemporaryBackendException("Unable to write mutations to accumulo table " + tableName, mre);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (MutationsRejectedException mre) {
                logger.error("Unable to close accumulo writer on table {}", tableName, mre);
            }
        }
    }

    private Collection<Mutation> convertMutations(Map<String, Map<StaticBuffer, KCVMutation>> mutations, final long putTimestamp, final long delTimestamp) {
        Map<StaticBuffer, Mutation> accMutByTitanKey = new HashMap<StaticBuffer, Mutation>();
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> mutsForStore: mutations.entrySet()) {
            byte[] mutCf = mutsForStore.getKey().getBytes(UTF_8);
            for (Map.Entry<StaticBuffer, KCVMutation> keyMut: mutsForStore.getValue().entrySet()) {
                Mutation accMut = accMutByTitanKey.get(keyMut.getKey());
                if (accMut == null) {
                    byte[] mutRow = keyMut.getKey().as(StaticBuffer.ARRAY_FACTORY);
                    accMut = new Mutation(mutRow);
                    accMutByTitanKey.put(keyMut.getKey(), accMut);
                }
                KCVMutation titanMut = keyMut.getValue();
                for (StaticBuffer sb: titanMut.getDeletions()) {
                    accMut.putDelete(mutCf, sb.as(StaticBuffer.ARRAY_FACTORY), delTimestamp);
                }
                for (Entry sb: titanMut.getAdditions()) {
                    accMut.put(mutCf, sb.getColumn().as(StaticBuffer.ARRAY_FACTORY), putTimestamp,
                            sb.getValue().as(StaticBuffer.ARRAY_FACTORY));
                }
            }
        }
        return accMutByTitanKey.values();
    }

    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new AccumuloTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        openStores.clear();
        if (logger.isTraceEnabled()) {
            openManagers.remove(this);
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        Configuration c = GraphDatabaseConfiguration.buildConfiguration();

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
                .orderedScan(true).unorderedScan(true).batchMutation(true)
                .multiQuery(true).distributed(true).keyOrdered(true).storeTTL(true)
                .timestamps(true).preferredTimestamps(Timestamps.MILLI).visibility(true)
                .keyConsistent(c);

        try {
            fb.localKeyPartition(getDeployment() == Deployment.LOCAL);
        } catch (Exception e) {
            logger.warn("Unexpected exception during getDeployment()", e);
        }

        return fb.build();
    }

    public void clearStorage() throws BackendException {
        try {
            newConnector().tableOperations().deleteRows(tableName, null, null);
        } catch (AccumuloException ae) {
            throw new PermanentBackendException("Unable to delete rows from table " + tableName, ae);
        } catch (AccumuloSecurityException ase) {
            throw new PermanentBackendException("Not permitted to delete rows from table " + tableName, ase);
        } catch (TableNotFoundException tnfe) {
            logger.warn("Table {} does not exist; can't clearStorage", tableName);
        }
    }

    @Override
    public String getName() {
        return tableName;
    }

    /**
     * Not sure this is actually very useful as the local key partition will probably change when the table is
     * compacted. Implemented for consistency with HBase (which probably has exactly the same constraint)
     * @return A list of KeyRanges which are hosted locally, or an empty list if no such ranges exist.
     * @throws BackendException
     */
    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        Map<String,Collection<KeyExtent>> rangesByHost = getRangesByHost();
        List<KeyRange> localPart = new LinkedList<KeyRange>();
        for (Map.Entry<String, Collection<KeyExtent>> ranges: rangesByHost.entrySet()) {
            String host = ranges.getKey().split(":")[0];
            if (NetworkUtil.isLocalConnection(host)) {
                for (KeyExtent ke: ranges.getValue()) {
                    localPart.add(newRangeFromExtent(ke));
                }
            };
        }
        return localPart;
    }

    private KeyRange newRangeFromExtent(KeyExtent ke) {
        // Accumulo's keyExtents are start-exclusive and end-inclusive.
        Text prevEndRow = ke.getPrevEndRow();
        Text endRow = ke.getEndRow();
        StaticBuffer endRowBuf;
        if ((endRow == null) || (endRow.getLength() == 0)) {
            endRowBuf = FOUR_ZERO_BYTES;
        } else {
            byte[] endRowExclusive = new byte[endRow.getLength() + 1];
            System.arraycopy(endRow.getBytes(), 0, endRowExclusive, 0, endRow.getLength());
            endRowBuf = StaticArrayBuffer.of(endRowExclusive);
        }
        StaticBuffer startRowBuf = ((prevEndRow == null) || (prevEndRow.getLength() == 0)) ? FOUR_ZERO_BYTES :
                new StaticArrayBuffer(ke.getPrevEndRow().copyBytes());
        return new KeyRange(startRowBuf, endRowBuf);
    }

    private Map<String,Collection<KeyExtent>> getRangesByHost() throws BackendException {
        final Random random = new Random();
        Connector accConn = newConnector();
        Map<String, String> tableIdByName = accConn.tableOperations().tableIdMap();
        String tableId = tableIdByName.get(tableName);
        Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
        List<Range> failures = new LinkedList<Range>();
        try {
            ClientContext ctx = newClientContext();
            TabletLocator tl = TabletLocator.getLocator(ctx, new Text(tableId));
            tl.invalidateCache();
            List<Range> ranges = Collections.singletonList(new Range());
            int numAttempts = 0;
            while (numAttempts++ < MAX_PARTITION_ATTEMPTS &&
                    !(failures = tl.binRanges(ctx, ranges, binnedRanges)).isEmpty()) {
                binnedRanges.clear();
                logger.warn("Unable to locate bins for specified ranges. Retrying.");
                Thread.sleep((int)(AVG_PARTITION_RETRY_MS * 2.0 * random.nextDouble()));
                tl.invalidateCache();
            }
        } catch (AccumuloException ae) {
            throw new TemporaryBackendException("Unable to load partitions for table " + tableName, ae);
        } catch (AccumuloSecurityException ase) {
            throw new TemporaryBackendException("Failed to load partitions for table " + tableName, ase);
        } catch (TableNotFoundException tnfe) {
            throw new PermanentBackendException("Table " + tableName + " does not exist", tnfe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        if (!failures.isEmpty()) {
            throw new TemporaryBackendException("Unable to get consistent view of tablet locations for table "
                    + tableName);
        }
        Map<String,Collection<KeyExtent>> out = new HashMap<String,Collection<KeyExtent>>();
        for ( Map.Entry<String,Map<KeyExtent,List<Range>>> e: binnedRanges.entrySet()) {
            out.put(e.getKey(), new TreeSet<KeyExtent>(e.getValue().keySet()));
        }
        return out;
    }

    @Override
    public String toString() {
        return "accumulo[" + tableName + "@" + super.toString() + "]";
    }

    public void dumpOpenManagers() {
        int numManagers = openManagers.size();
        logger.trace("---- Begin open Accumulo store manager list ({} managers) ----", numManagers);
        for (AccumuloStoreManager asm: openManagers.keySet()) {
            logger.trace("Manager {} opened at: ", asm, openManagers.get(asm));
        }
        logger.trace("---- End open Accumulo store manager list ({} managers) ----", numManagers);
    }

    private void checkConfigDeprecation(Configuration config) {
        if (config.has(GraphDatabaseConfiguration.STORAGE_PORT)) {
            logger.warn("The configuration property {} is ignored for Accumulo. Set {} in client.conf or {}.{} in Titan's configuration file.",
                    ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_PORT),
                    ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST, ConfigElement.getPath(ACCUMULO_CONFIGURATION_NAMESPACE),
                    ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST);
        }
    }

    private ClientContext newClientContext() {
        return new ClientContext(accInstance, new Credentials(this.username, new PasswordToken(this.password)),
                clientConf);
    }

    private Connector newConnector() throws BackendException {
        try {
            return accInstance.getConnector(this.username, new PasswordToken(this.password));
        } catch (AccumuloException ae) {
            throw new TemporaryBackendException("Unable to establish connection to Accumulo", ae);
        } catch (AccumuloSecurityException ase) {
            throw new TemporaryBackendException("Failed to connect to Accumulo", ase);
        }
    }

    BatchScanner newBatchScanner(ScanConfig cfg) throws BackendException {
        try {
            BatchScanner scanner = newConnector().createBatchScanner(tableName, scanAuths, numScanThreads);
            scanner.fetchColumnFamily(cfg.colFam);
            IteratorSetting ttlSetting = storeTtls.get(cfg.colFam.toString());
            if (ttlSetting != null) {
                scanner.addScanIterator(ttlSetting);
            }
            for (IteratorSetting iterSetting: cfg.getIteratorSettings()) {
                scanner.addScanIterator(iterSetting);
            }
            return scanner;
        } catch (TableNotFoundException tnfe) {
            throw new PermanentBackendException("Unable to create scanner for table " + tableName, tnfe);
        }
    }

    static class ScanConfig {
        private boolean onlyLatestVersion;
        private Text colFam;
        private SliceQuery sliceQuery;
        private boolean wholeRow;

        ScanConfig(Text colFam) {
            colFam = new Text(colFam);
        }

        ScanConfig onlyLatestVersion() {
            onlyLatestVersion = true;
            return this;
        }

        ScanConfig setCqSlice(SliceQuery sliceQuery) {
            this.sliceQuery = sliceQuery;
            return this;
        }

        ScanConfig enableWholeRow() {
            wholeRow = true;
            return this;
        }

        List<IteratorSetting> getIteratorSettings() {
            List<IteratorSetting> settings = new LinkedList<IteratorSetting>();
            if (onlyLatestVersion) {
                settings.add(LATEST_VERSION);
            }
            if (sliceQuery != null) {
                byte[] colStartBytes = sliceQuery.getSliceEnd().length() > 0 ?
                        sliceQuery.getSliceStart().as(StaticBuffer.ARRAY_FACTORY) : null;
                byte[] colEndBytes = sliceQuery.getSliceEnd().length() > 0 ?
                        sliceQuery.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY) : null;
                IteratorSetting cqSliceSetting = new IteratorSetting(PRIORITY_CQ_SLICE, "CqSlice", ColumnSliceFilter.class);
                ColumnSliceFilter.setSlice(cqSliceSetting, colStartBytes == null ? null : new String(colStartBytes), true,
                        colEndBytes == null ? null : new String(colEndBytes), false);
                settings.add(cqSliceSetting);
            }
            if (wholeRow) {
                settings.add(new IteratorSetting(PRIORITY_WHOLE_ROW, "WholeRow", WholeRowIterator.class));
            }
            return settings;
        }
    }
}

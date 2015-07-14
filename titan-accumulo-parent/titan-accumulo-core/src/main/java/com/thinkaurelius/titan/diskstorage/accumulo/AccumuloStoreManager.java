package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Joiner;
import com.sun.corba.se.impl.orbutil.graph.Graph;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StoreMetaData;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AccumuloStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloStoreManager.class);

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

    public static final ConfigNamespace ACCUMULO_CONFIGURATION_NAMESPACE =
            new ConfigNamespace(ACCUMULO_NS, "ext", "Overrides for client.conf options", true);

    private static final ConcurrentHashMap<AccumuloStoreManager, Throwable> openManagers =
            new ConcurrentHashMap<AccumuloStoreManager, Throwable>();

    public static final int PORT_DEFAULT = 9160;

    private final ConcurrentHashMap<String, AccumuloKeyColumnValueStore> openStores;
    private final String tableName;
    private final boolean skipSchemaCheck;
    private final ClientConfiguration clientConf;
    private final Instance accInstance;

    public AccumuloStoreManager(Configuration storageConfig) throws BackendException {
        super(storageConfig, PORT_DEFAULT);
        tableName = storageConfig.get(ACCUMULO_TABLE);
        skipSchemaCheck = storageConfig.get(SKIP_SCHEMA_CHECK);
        clientConf = loadClientConfig(storageConfig);
        accInstance = new ZooKeeperInstance(clientConf);

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
        if (!CLIENT_CONF_FILE_DEFAULT.equals(clientConfFile)) {
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
            logger.debug("Dumping Accumulo client configuration");
            logger.debug(clientConf.serialize());
            logger.debug("End of Accumulo client configuration");
        }
        return clientConf;
    }

    @Override
    public Deployment getDeployment() {
        // TODO: figure out the intent of getLocalKeyPartition - if *some* of the key space is local, is that sufficient
        // to return Deployment.LOCAL here (as it appears to be in HBase?)
        return Deployment.REMOTE;
    }

    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        return new AccumuloKeyColumnValueStore();
    }

    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {

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
        return null;
    }

    public void clearStorage() throws BackendException {

    }

    @Override
    public String getName() {
        return tableName;
    }

    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        Connector accConn = newConnector();
        Map<String, String> tableIdByName = accConn.tableOperations().tableIdMap();
        String tableId = tableIdByName.get(tableName);
        Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
        try {
            ClientContext ctx = newClientContext();
            TabletLocator tl = TabletLocator.getLocator(ctx, new Text(tableId));
            tl.invalidateCache();
            while (!tl.binRanges(ctx, ranges, binnedRanges).isEmpty()) {
                if (!Tables.exists(instance, tableId))
                    throw new TableDeletedException(tableId);
                if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
                    throw new TableOfflineException(instance, tableId);
            }
        } finally {

        }
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
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new PermanentBackendException("Unable to establish connection to Accumulo", e);
        }
    }
}

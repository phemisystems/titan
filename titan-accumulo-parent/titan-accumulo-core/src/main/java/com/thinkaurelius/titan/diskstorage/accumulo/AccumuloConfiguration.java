package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class AccumuloConfiguration {
    public static final ConfigNamespace ACCUMULO_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "accumulo", "Accumulo storage options");

    public static final ConfigNamespace ACC_CONF =
            new ConfigNamespace(ACCUMULO_NS, "ext", "Overrides for client.conf options");

    public static final ConfigNamespace INST = new ConfigNamespace(ACC_CONF, "instance", "Instance-specific options");

    public static final ConfigNamespace ZK = new ConfigNamespace(INST, "zookeeper", "Zookeeper-specific options");

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

    static final String CLIENT_CONF_FILE_DEFAULT = "-DEFAULT-";

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


    public static final ConfigOption INSTANCE_NAME = new ConfigOption<String>(INST, "name", "Accumulo instance name",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption ZK_HOST = new ConfigOption<String>(ZK, "host",
            "Zookeeper connection string eg. server1:2181,server2:2181,server3:2181", ConfigOption.Type.LOCAL,
            String.class);
}

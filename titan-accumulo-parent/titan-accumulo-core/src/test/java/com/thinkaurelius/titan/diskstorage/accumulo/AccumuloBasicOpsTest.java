package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.util.concurrent.Uninterruptibles;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class AccumuloBasicOpsTest {

  private static MiniAccumuloCluster mac;
  private static TitanGraph g;

  @BeforeClass
  public static void setupClass() throws Exception {
    Path macPath = Files.createTempDirectory("mac");
    System.out.println("MAC running at " + macPath);
    MiniAccumuloConfig macCfg = new MiniAccumuloConfig(macPath.toFile(), "secret");
    macCfg.setZooKeeperPort(2181);
    mac = new MiniAccumuloCluster(macCfg);
    mac.start();
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    g = newGraphFromMac();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    g.shutdown();
    mac.stop();
  }

  @Test
  public void testDefinePropertyKey() throws Exception {
    TitanGraph g = newGraphFromMac();
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    TitanManagement mgmt = g.getManagementSystem();
    mgmt.makePropertyKey("name").dataType(String.class).make();
    mgmt.commit();
    mgmt = g.getManagementSystem();
    PropertyKey key = mgmt.getPropertyKey("name");
    mgmt.commit();
    assertTrue("PropertyKey must exist, must be retrievable", key != null);
    g.shutdown();
  }

  @Test
  public void testSetPropertyKey() throws Exception {
    TitanTransaction tx = g.newTransaction();
    // vertices

    Vertex saturn = tx.addVertexWithLabel("titan");
    saturn.setProperty("name", "saturn");
    tx.commit();
  }
  private static TitanGraph newGraphFromMac() throws Exception {
    MiniAccumuloConfig macCfg = mac.getConfig();
    Configuration cfg = new BaseConfiguration();
    cfg.addProperty("storage.accumulo.ext.instance.zookeeper.host", "localhost:" + macCfg.getZooKeeperPort());
    cfg.addProperty("storage.accumulo.ext.instance.name", macCfg.getInstanceName());
    cfg.addProperty("storage.username", "root");
    cfg.addProperty("storage.password", macCfg.getRootPassword());
    cfg.addProperty("storage.backend", "accumulo");
    cfg.addProperty("cache.db-cache", "true");
    cfg.addProperty("cache.db-cache-clean-wait", "20");
    cfg.addProperty("cache.db-cache-time", "180000");
    cfg.addProperty("cache.db-cache-size", "0.5");
//    cfg.addProperty("index.search.backend", "lucene");
//    cfg.addProperty("index.search.directory", "/tmp/searchindex");
    return TitanFactory.open(cfg);
  }
}

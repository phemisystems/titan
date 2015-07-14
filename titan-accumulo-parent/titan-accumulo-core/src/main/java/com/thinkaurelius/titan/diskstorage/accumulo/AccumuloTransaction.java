package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

/**
 * No-op implementation just intended to provide type safety.
 */
public class AccumuloTransaction extends AbstractStoreTransaction {

  public AccumuloTransaction(final BaseTransactionConfig config) {
    super(config);
  }

}

package com.uber.data.kafka.connect.ureplicator3.coordination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignmentStores {

  private static final Logger log = LoggerFactory.getLogger(AssignmentStores.class);

  private static AssignmentStore store;

  public static synchronized void initialize(AssignmentStoreConfig config) {
    if (store == null)
      store = initializeStore(config);
    else
      log.warn("Store is already initialized; this should never happen outside an embedded testing environment");
  }

  // TODO: Instead of exposing the whole store, we could expose a safe set of operations
  //       like assign, revoke, and addListener here (this would prevent callers from doing
  //       something stupid like trying to close the store)
  public static AssignmentStore get() {
    if (store == null) {
      throw new IllegalStateException("Store has not been initialized yet");
    }
    return store;
  }

  // TODO: private void destroyStore()

  private static AssignmentStore initializeStore(AssignmentStoreConfig config) {
    log.info("Initializing assignment store");
    AssignmentStore result = new AssignmentStore(config);
    result.start();
    log.info("Finished initializing assignment store");
    return result;
  }

}

package com.uber.data.kafka.connect.utils;

import com.uber.data.kafka.exception.ResolverException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BrokerResolverTest {

  private static final String SECPOC_DCA = "kafka-secpoc.dca.uber.internal:9092";

  private static final String DEV1_PHX = "kafka-dev1.phx.uber.internal:9092";

  @Test
  public void testBrokerResolver() {
    assertResolution("dca-secpoc", SECPOC_DCA);
    assertResolution("dca1-secpoc", SECPOC_DCA);
    assertResolution("kloak-dca-secpoc", SECPOC_DCA);
    assertResolution("kloak-dca1-secpoc", SECPOC_DCA);

    assertResolution("phx-dev1", DEV1_PHX);
    assertResolution("phx2-dev1", DEV1_PHX);
    assertResolution("kloak-phx-dev1", DEV1_PHX);
    assertResolution("kloak-phx2-dev1", DEV1_PHX);

    assertFailedResolution(null);
    assertFailedResolution("");
    assertFailedResolution("idklol");
  }

  private void assertResolution(String clusterName, String expected) {
    String actual = assertDoesNotThrow(() -> BrokerResolver.resolveBootstrap(clusterName));
    assertEquals(expected, actual);
  }

  private void assertFailedResolution(String clusterName) {
    assertThrows(ResolverException.class, () -> BrokerResolver.resolveBootstrap(clusterName));
  }

}

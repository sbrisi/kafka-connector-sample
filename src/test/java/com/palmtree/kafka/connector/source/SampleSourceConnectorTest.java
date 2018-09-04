package com.palmtree.kafka.connector.source;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.palmtree.kafka.connector.common.ConnectorUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link SampleSourceConnector}. */
public class SampleSourceConnectorTest {

  private static final int NUM_TASKS = 10;
  private static final String CPS_PROJECT = "hello";
  private static final String CPS_SUBSCRIPTION = "big";
  private static final String KAFKA_TOPIC = "world";

  private SampleSourceConnector connector;
  private Map<String, String> props;

  @Before
  public void setup() {
    connector = spy(new SampleSourceConnector());
    props = new HashMap<>();
    props.put(SampleSourceConnector.CPS_SUBSCRIPTION_CONFIG, CPS_SUBSCRIPTION);
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(SampleSourceConnector.KAFKA_TOPIC_CONFIG, KAFKA_TOPIC);
  }

  @Test(expected = ConnectException.class)
  public void testStartWhenSubscriptionNonexistant() {
    doThrow(new ConnectException("")).when(connector).verifySubscription(anyString(), anyString());
    connector.start(props);
  }

  @Test(expected = ConfigException.class)
  public void testStartWhenRequiredConfigMissing() {
    connector.start(new HashMap<String, String>());
  }

  @Test
  public void testTaskConfigs() {
    doNothing().when(connector).verifySubscription(anyString(), anyString());
    connector.start(props);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(NUM_TASKS);
    assertEquals(taskConfigs.size(), NUM_TASKS);
    for (int i = 0; i < taskConfigs.size(); ++i) {
      assertEquals(taskConfigs.get(i), props);
    }
  }

  @Test
  public void testSourceConnectorTaskClass() {
    assertEquals(SampleSourceTask.class, connector.taskClass());
  }
}

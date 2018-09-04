package com.palmtree.kafka.connector.sink;

import static org.junit.Assert.assertEquals;

import com.palmtree.kafka.connector.common.ConnectorUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/** Tests for {@link SampleSinkConnector}. */
public class SampleSinkConnectorTest {

  private static final int NUM_TASKS = 10;
  private static final String CPS_PROJECT = "hello";
  private static final String CPS_TOPIC = "world";

  private SampleSinkConnector connector;
  private Map<String, String> props;

  @Before
  public void setup() {
    connector = new SampleSinkConnector();
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
  }

  @Test
  public void testTaskConfigs() {
    connector.start(props);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(NUM_TASKS);
    assertEquals(taskConfigs.size(), NUM_TASKS);
    for (int i = 0; i < taskConfigs.size(); ++i) {
      assertEquals(taskConfigs.get(i), props);
    }
  }

  @Test
  public void testTaskClass() {
    assertEquals(SampleSinkTask.class, connector.taskClass());
  }
}

package com.palmtree.kafka.connector.source;

import com.google.common.annotations.VisibleForTesting;
import com.palmtree.kafka.connector.common.ConnectorUtils;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SourceConnector} that writes messages to a specific topic in <a
 * href="http://kafka.apache.org/">Apache Kafka</a>.
 */
public class SampleSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(SampleSourceConnector.class);

  public static final String KAFKA_PARTITIONS_CONFIG = "kafka.partition.count";
  public static final String KAFKA_PARTITION_SCHEME_CONFIG = "kafka.partition.scheme";
  public static final String KAFKA_MESSAGE_KEY_CONFIG = "kafka.key.attribute";
  public static final String KAFKA_MESSAGE_TIMESTAMP_CONFIG = "kafka.timestamp.attribute";
  public static final String KAFKA_TOPIC_CONFIG = "kafka.topic";
  public static final String CPS_SUBSCRIPTION_CONFIG = "cps.subscription";
  public static final String CPS_MAX_BATCH_SIZE_CONFIG = "cps.maxBatchSize";
  public static final int DEFAULT_CPS_MAX_BATCH_SIZE = 100;
  public static final int DEFAULT_KAFKA_PARTITIONS = 1;
  public static final String DEFAULT_KAFKA_PARTITION_SCHEME = "round_robin";

  /** Defines the accepted values for the {@link #KAFKA_PARTITION_SCHEME_CONFIG}. */
  public enum PartitionScheme {
    ROUND_ROBIN("round_robin"),
    HASH_KEY("hash_key"),
    HASH_VALUE("hash_value");

    private String value;

    PartitionScheme(String value) {
      this.value = value;
    }

    public String toString() {
      return value;
    }

    public static PartitionScheme getEnum(String value) {
      if (value.equals("round_robin")) {
        return PartitionScheme.ROUND_ROBIN;
      } else if (value.equals("hash_key")) {
        return PartitionScheme.HASH_KEY;
      } else if (value.equals("hash_value")) {
        return PartitionScheme.HASH_VALUE;
      } else {
        return null;
      }
    }

    /** Validator class for {@link SampleSourceConnector.PartitionScheme}. */
    public static class Validator implements ConfigDef.Validator {

      @Override
      public void ensureValid(String name, Object o) {
        String value = (String) o;
        if (!value.equals(SampleSourceConnector.PartitionScheme.ROUND_ROBIN.toString())
            && !value.equals(SampleSourceConnector.PartitionScheme.HASH_VALUE.toString())
            && !value.equals(SampleSourceConnector.PartitionScheme.HASH_KEY.toString())) {
          throw new ConfigException(
              "Valid values for "
                  + SampleSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG
                  + " are hash_value, hash_key and round_robin");
        }
      }
    }
  }

  private Map<String, String> props;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    // Do a validation of configs here too so that we do not pass null objects to
    // verifySubscription().
    config().parse(props);
    String cpsProject = props.get(ConnectorUtils.CPS_PROJECT_CONFIG);
    String cpsSubscription = props.get(CPS_SUBSCRIPTION_CONFIG);
    verifySubscription(cpsProject, cpsSubscription);
    this.props = props;
    log.info("Started the SampleSourceConnector");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SampleSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Each task will get the exact same configuration. Delegate config validation to the task.
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>(props);
      configs.add(config);
    }
    return configs;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(
            KAFKA_TOPIC_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The topic in Kafka which will receive messages that were pulled from Cloud Pub/Sub.")
        .define(
            ConnectorUtils.CPS_PROJECT_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The project containing the topic from which to pull messages.")
        .define(
            CPS_SUBSCRIPTION_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The name of the subscription to Cloud Pub/Sub.")
        .define(
            CPS_MAX_BATCH_SIZE_CONFIG,
            Type.INT,
            DEFAULT_CPS_MAX_BATCH_SIZE,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The minimum number of messages to batch per pull request to Cloud Pub/Sub.")
        .define(
            KAFKA_MESSAGE_KEY_CONFIG,
            Type.STRING,
            null,
            Importance.MEDIUM,
            "The Cloud Pub/Sub message attribute to use as a key for messages published to Kafka.")
        .define(
            KAFKA_MESSAGE_TIMESTAMP_CONFIG,
            Type.STRING,
            null,
            Importance.MEDIUM,
            "The optional Cloud Pub/Sub message attribute to use as a timestamp for messages "
                + "published to Kafka. The timestamp is Long value.")
        .define(
            KAFKA_PARTITIONS_CONFIG,
            Type.INT,
            DEFAULT_KAFKA_PARTITIONS,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The number of Kafka partitions for the Kafka topic in which messages will be "
                + "published to.")
        .define(
            KAFKA_PARTITION_SCHEME_CONFIG,
            Type.STRING,
            DEFAULT_KAFKA_PARTITION_SCHEME,
            new PartitionScheme.Validator(),
            Importance.MEDIUM,
            "The scheme for assigning a message to a partition in Kafka.");
  }

  /**
   * Check whether the user provided Cloud Pub/Sub subscription name specified by {@link
   * #CPS_SUBSCRIPTION_CONFIG} exists or not.
   */
  @VisibleForTesting
  public void verifySubscription(String cpsProject, String cpsSubscription) {
    try {
      SubscriberFutureStub stub = SubscriberGrpc.newFutureStub(ConnectorUtils.getChannel());
      GetSubscriptionRequest request =
          GetSubscriptionRequest.newBuilder()
              .setSubscription(
                  String.format(
                      ConnectorUtils.CPS_SUBSCRIPTION_FORMAT, cpsProject, cpsSubscription))
              .build();
      stub.getSubscription(request).get();
    } catch (Exception e) {
      throw new ConnectException(
          "Error verifying the subscription " + cpsSubscription + " for project " + cpsProject, e);
    }
  }

  @Override
  public void stop() {}
}

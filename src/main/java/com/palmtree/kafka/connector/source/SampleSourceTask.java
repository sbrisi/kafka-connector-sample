package com.palmtree.kafka.connector.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.util.Timestamps;
import com.palmtree.kafka.connector.common.ConnectorUtils;
import com.palmtree.kafka.connector.source.SampleSourceConnector.PartitionScheme;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SourceTask} used by a {@link SampleSourceConnector} to write messages to <a
 * href="http://kafka.apache.org/">Apache Kafka</a>. Due to at-last-once semantics in Google
 * Cloud Pub/Sub duplicates in Kafka are possible.
 */
public class SampleSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(SampleSourceTask.class);
  private static final int NUM_CPS_SUBSCRIBERS = 10;

  private String kafkaTopic;
  private String cpsSubscription;
  private String kafkaMessageKeyAttribute;
  private String kafkaMessageTimestampAttribute;
  private int kafkaPartitions;
  private PartitionScheme kafkaPartitionScheme;
  private int cpsMaxBatchSize;
  // Keeps track of the current partition to publish to if the partition scheme is round robin.
  private int currentRoundRobinPartition = -1;
  // Keep track of all ack ids that have not been sent correctly acked yet.
  private Set<String> deliveredAckIds = Collections.synchronizedSet(new HashSet<String>());
  private Set<String> ackIds = Collections.synchronizedSet(new HashSet<String>());
  private SampleSubscriber subscriber;
  private Set<String> ackIdsInFlight = Collections.synchronizedSet(new HashSet<String>());
  private final Set<String> standardAttributes = new HashSet<>();

  public SampleSourceTask() {}

  @VisibleForTesting
  public SampleSourceTask(SampleSubscriber subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public String version() {
    return new SampleSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    Map<String, Object> validatedProps = new SampleSourceConnector().config().parse(props);
    cpsSubscription =
        String.format(
            ConnectorUtils.CPS_SUBSCRIPTION_FORMAT,
            validatedProps.get(ConnectorUtils.CPS_PROJECT_CONFIG).toString(),
            validatedProps.get(SampleSourceConnector.CPS_SUBSCRIPTION_CONFIG).toString());
    kafkaTopic = validatedProps.get(SampleSourceConnector.KAFKA_TOPIC_CONFIG).toString();
    cpsMaxBatchSize =
        (Integer) validatedProps.get(SampleSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG);
    kafkaPartitions =
        (Integer) validatedProps.get(SampleSourceConnector.KAFKA_PARTITIONS_CONFIG);
    kafkaMessageKeyAttribute =
        (String) validatedProps.get(SampleSourceConnector.KAFKA_MESSAGE_KEY_CONFIG);
    kafkaMessageTimestampAttribute =
        (String) validatedProps.get(SampleSourceConnector.KAFKA_MESSAGE_TIMESTAMP_CONFIG);
    kafkaPartitionScheme =
        PartitionScheme.getEnum(
            (String) validatedProps.get(SampleSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG));
    if (subscriber == null) {
      // Only do this if we did not set through the constructor.
      subscriber = new SampleRoundRobinSubscriber(NUM_CPS_SUBSCRIBERS);
    }
    standardAttributes.add(kafkaMessageKeyAttribute);
    standardAttributes.add(kafkaMessageTimestampAttribute);
    log.info("Started a SampleSourceTask.");
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    ackMessages();
    log.debug("Polling...");
    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(cpsSubscription)
            .setReturnImmediately(false)
            .setMaxMessages(cpsMaxBatchSize)
            .build();
    try {
      PullResponse response = subscriber.pull(request).get();
      List<SourceRecord> sourceRecords = new ArrayList<>();
      log.trace("Received " + response.getReceivedMessagesList().size() + " messages");
      for (ReceivedMessage rm : response.getReceivedMessagesList()) {
        PubsubMessage message = rm.getMessage();
        String ackId = rm.getAckId();
        // If we are receiving this message a second (or more) times because the ack for it failed
        // then do not create a SourceRecord for this message. In case we are waiting for ack
        // response we also skip the message
        if (ackIds.contains(ackId) || deliveredAckIds.contains(ackId) || ackIdsInFlight.contains(ackId)) {
          continue;
        }
        ackIds.add(ackId);
        Map<String, String> messageAttributes = message.getAttributes();
        String key = messageAttributes.get(kafkaMessageKeyAttribute);
        Long timestamp = getLongValue(messageAttributes.get(kafkaMessageTimestampAttribute));
        if (timestamp == null){
          timestamp = Timestamps.toMillis(message.getPublishTime());
        }
        ByteString messageData = message.getData();
        byte[] messageBytes = messageData.toByteArray();

        boolean hasCustomAttributes = !standardAttributes.containsAll(messageAttributes.keySet());

        Map<String,String> ack = Collections.singletonMap(cpsSubscription, ackId);
        SourceRecord record = null;
        if (hasCustomAttributes) {
          SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().field(
              ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD,
              Schema.BYTES_SCHEMA);

          for (Entry<String, String> attribute :
               messageAttributes.entrySet()) {
            if (!attribute.getKey().equals(kafkaMessageKeyAttribute)) {
              valueSchemaBuilder.field(attribute.getKey(),
                                       Schema.STRING_SCHEMA);
            }
          }

          Schema valueSchema = valueSchemaBuilder.build();
          Struct value =
              new Struct(valueSchema)
                  .put(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD,
                       messageBytes);
          for (Field field : valueSchema.fields()) {
            if (!field.name().equals(
                    ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD)) {
              value.put(field.name(), messageAttributes.get(field.name()));
            }
          }
          record =
            new SourceRecord(
                null,
                ack,
                kafkaTopic,
                selectPartition(key, value),
                Schema.OPTIONAL_STRING_SCHEMA,
                key,
                valueSchema,
                value,
                timestamp);
        } else {
          record =
            new SourceRecord(
                null,
                ack,
                kafkaTopic,
                selectPartition(key, messageBytes),
                Schema.OPTIONAL_STRING_SCHEMA,
                key,
                Schema.BYTES_SCHEMA,
                messageBytes,
                timestamp);
        }
        sourceRecords.add(record);
      }
      return sourceRecords;
    } catch (Exception e) {
      log.info("Error while retrieving records, treating as an empty poll. " + e);
      return new ArrayList<>();
    }
  }

  @Override
  public void commit() throws InterruptedException {
    ackMessages();
  }

  /**
   * Attempt to ack all ids in {@link #deliveredAckIds}.
   */
  private void ackMessages() {
    if (deliveredAckIds.size() != 0) {
      AcknowledgeRequest.Builder requestBuilder = AcknowledgeRequest.newBuilder()
          .setSubscription(cpsSubscription);
      final Set<String> ackIdsBatch = new HashSet<>();
      synchronized (deliveredAckIds) {
        requestBuilder.addAllAckIds(deliveredAckIds);
        ackIdsInFlight.addAll(deliveredAckIds);
        ackIdsBatch.addAll(deliveredAckIds);
        deliveredAckIds.clear();
      }
      ListenableFuture<Empty> response = subscriber.ackMessages(requestBuilder.build());
      Futures.addCallback(
          response,
          new FutureCallback<Empty>() {
            @Override
            public void onSuccess(Empty result) {
              ackIdsInFlight.removeAll(ackIdsBatch);
              log.trace("Successfully acked a set of messages. {}", ackIdsBatch.size());
            }

            @Override
            public void onFailure(Throwable t) {
              deliveredAckIds.addAll(ackIdsBatch);
              ackIdsInFlight.removeAll(ackIdsBatch);
              log.error("An exception occurred acking messages: " + t);
            }
          });
    }
  }

  /** Return the partition a message should go to based on {@link #kafkaPartitionScheme}. */
  private int selectPartition(Object key, Object value) {
    if (kafkaPartitionScheme.equals(PartitionScheme.HASH_KEY)) {
      return key == null ? 0 : Math.abs(key.hashCode()) % kafkaPartitions;
    } else if (kafkaPartitionScheme.equals(PartitionScheme.HASH_VALUE)) {
      return Math.abs(value.hashCode()) % kafkaPartitions;
    } else {
      currentRoundRobinPartition = ++currentRoundRobinPartition % kafkaPartitions;
      return currentRoundRobinPartition;
    }
  }

  private Long getLongValue(String timestamp) {
    if (timestamp == null) {
      return null;
    }
    try {
      return Long.valueOf(timestamp);
    } catch (NumberFormatException e) {
      log.error("Error while converting `{}` to number", timestamp, e);
    }
    return null;
  }

  @Override
  public void stop() {}

  @Override
  public void commitRecord(SourceRecord record) {
    String ackId = record.sourceOffset().get(cpsSubscription).toString();
    deliveredAckIds.add(ackId);
    ackIds.remove(ackId);
    log.trace("Committed {}", ackId);
  }
}

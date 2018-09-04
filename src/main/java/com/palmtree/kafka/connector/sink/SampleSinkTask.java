package com.palmtree.kafka.connector.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.palmtree.kafka.connector.common.ConnectorUtils;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * A {@link SinkTask} used by a {@link SampleSinkConnector} to write messages to <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class SampleSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(SampleSinkTask.class);

  // Maps a topic to another map which contains the outstanding futures per partition
  private Map<String, Map<Integer, OutstandingFuturesForPartition>> allOutstandingFutures =
      new HashMap<>();
  private String cpsProject;
  private String cpsTopic;
  private String messageBodyName;
  private long maxBufferSize;
  private long maxBufferBytes;
  private int maxDelayThresholdMs;
  private int maxRequestTimeoutMs;
  private int maxTotalTimeoutMs;
  private boolean includeMetadata;
  private com.google.cloud.pubsub.v1.Publisher publisher;

  /** Holds a list of the publishing futures that have not been processed for a single partition. */
  private class OutstandingFuturesForPartition {
    public List<ApiFuture<String>> futures = new ArrayList<>();
  }

  /**
   * Holds a list of the unpublished messages for a single partition and the total size in bytes of
   * the messages in the list.
   */
  private class UnpublishedMessagesForPartition {
    public List<PubsubMessage> messages = new ArrayList<>();
    public int size = 0;
  }

  public SampleSinkTask() {}

  @VisibleForTesting
  public SampleSinkTask(Publisher publisher) {
    this.publisher = publisher;
  }

  @Override
  public String version() {
    return new SampleSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    Map<String, Object> validatedProps = new SampleSinkConnector().config().parse(props);
    cpsProject = validatedProps.get(ConnectorUtils.CPS_PROJECT_CONFIG).toString();
    cpsTopic = validatedProps.get(ConnectorUtils.CPS_TOPIC_CONFIG).toString();
    maxBufferSize = (Integer) validatedProps.get(SampleSinkConnector.MAX_BUFFER_SIZE_CONFIG);
    maxBufferBytes = (Long) validatedProps.get(SampleSinkConnector.MAX_BUFFER_BYTES_CONFIG);
    maxDelayThresholdMs =
        (Integer) validatedProps.get(SampleSinkConnector.MAX_DELAY_THRESHOLD_MS);
    maxRequestTimeoutMs =
        (Integer) validatedProps.get(SampleSinkConnector.MAX_REQUEST_TIMEOUT_MS);
    maxTotalTimeoutMs =
        (Integer) validatedProps.get(SampleSinkConnector.MAX_TOTAL_TIMEOUT_MS);
    messageBodyName = (String) validatedProps.get(SampleSinkConnector.CPS_MESSAGE_BODY_NAME);
    includeMetadata = (Boolean) validatedProps.get(SampleSinkConnector.PUBLISH_KAFKA_METADATA);
    if (publisher == null) {
      // Only do this if we did not use the constructor.
      createPublisher();
    }
    log.info("Start SampleSinkTask");
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    log.debug("Received " + sinkRecords.size() + " messages to send to CPS.");
    PubsubMessage.Builder builder = PubsubMessage.newBuilder();
    for (SinkRecord record : sinkRecords) {
      log.trace("Received record: " + record.toString());
      Map<String, String> attributes = new HashMap<>();
      ByteString value = handleValue(record.valueSchema(), record.value(), attributes);
      if (record.key() != null) {
        String key = record.key().toString();
        attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, key);
      }
      if (includeMetadata) {
        attributes.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, record.topic());
        attributes.put(
            ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, record.kafkaPartition().toString());
        attributes.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, Long.toString(record.kafkaOffset()));
        attributes.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, record.timestamp().toString());
      }
      PubsubMessage message = builder.setData(value).putAllAttributes(attributes).build();
      publishMessage(record.topic(), record.kafkaPartition(), message);
    }
  }

  private ByteString handleValue(Schema schema, Object value, Map<String, String> attributes) {
    if (schema == null) {
      String str = value.toString();
      return ByteString.copyFromUtf8(str);
    }
    Schema.Type t = schema.type();
    switch (t) {
      case INT8:
        byte b = (Byte) value;
        byte[] arr = {b};
        return ByteString.copyFrom(arr);
      case INT16:
        ByteBuffer shortBuf = ByteBuffer.allocate(2);
        shortBuf.putShort((Short) value);
        return ByteString.copyFrom(shortBuf);
      case INT32:
        ByteBuffer intBuf = ByteBuffer.allocate(4);
        intBuf.putInt((Integer) value);
        return ByteString.copyFrom(intBuf);
      case INT64:
        ByteBuffer longBuf = ByteBuffer.allocate(8);
        longBuf.putLong((Long) value);
        return ByteString.copyFrom(longBuf);
      case FLOAT32:
        ByteBuffer floatBuf = ByteBuffer.allocate(4);
        floatBuf.putFloat((Float) value);
        return ByteString.copyFrom(floatBuf);
      case FLOAT64:
        ByteBuffer doubleBuf = ByteBuffer.allocate(8);
        doubleBuf.putDouble((Double) value);
        return ByteString.copyFrom(doubleBuf);
      case BOOLEAN:
        byte bool = (byte) ((Boolean) value ? 1 : 0);
        byte[] boolArr = {bool};
        return ByteString.copyFrom(boolArr);
      case STRING:
        String str = (String) value;
        return ByteString.copyFromUtf8(str);
      case BYTES:
        if (value instanceof ByteString) {
          return (ByteString) value;
        } else if (value instanceof byte[]) {
          return ByteString.copyFrom((byte[]) value);
        } else if (value instanceof ByteBuffer) {
          return ByteString.copyFrom((ByteBuffer) value);
        } else {
          throw new DataException("Unexpected value class with BYTES schema type.");
        }
      case STRUCT:
        Struct struct = (Struct) value;
        ByteString msgBody = null;
        for (Field f : schema.fields()) {
          Schema.Type fieldType = f.schema().type();
          if (fieldType == Type.MAP || fieldType == Type.STRUCT) {
            throw new DataException("Struct type does not support nested Map or Struct types, " +
                "present in field " + f.name());
          }

          Object val = struct.get(f);
          if (val == null) {
            if (!f.schema().isOptional()) {
              throw new DataException("Struct message missing required field " + f.name());
            }  else {
              continue;
            }
          }
          if (f.name().equals(messageBodyName)) {
            Schema bodySchema = f.schema();
            msgBody = handleValue(bodySchema, val, null);
          } else {
            attributes.put(f.name(), val.toString());
          }
        }
        if (msgBody != null) {
          return msgBody;
        } else {
          return ByteString.EMPTY;
        }
      case MAP:
        Map<Object, Object> map = (Map<Object, Object>) value;
        Set<Object> keys = map.keySet();
        ByteString mapBody = null;
        for (Object key : keys) {
          if (key.equals(messageBodyName)) {
            mapBody = ByteString.copyFromUtf8(map.get(key).toString());
          } else {
            attributes.put(key.toString(), map.get(key).toString());
          }
        }
        if (mapBody != null) {
          return mapBody;
        } else {
          return ByteString.EMPTY;
        }
      case ARRAY:
        Schema.Type arrType = schema.valueSchema().type();
        if (arrType == Type.MAP || arrType == Type.STRUCT) {
          throw new DataException("Array type does not support Map or Struct types.");
        }
        ByteString out = ByteString.EMPTY;
        Object[] objArr = (Object[]) value;
        for (Object o : objArr) {
          out = out.concat(handleValue(schema.valueSchema(), o, null));
        }
        return out;
    }
    return ByteString.EMPTY;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
    log.debug("Flushing...");
    // Process results of all the outstanding futures specified by each TopicPartition.
    for (Map.Entry<TopicPartition, OffsetAndMetadata> partitionOffset :
        partitionOffsets.entrySet()) {
      log.trace("Received flush for partition " + partitionOffset.getKey().toString());
      Map<Integer, OutstandingFuturesForPartition> outstandingFuturesForTopic =
          allOutstandingFutures.get(partitionOffset.getKey().topic());
      if (outstandingFuturesForTopic == null) {
        continue;
      }
      OutstandingFuturesForPartition outstandingFutures =
          outstandingFuturesForTopic.get(partitionOffset.getKey().partition());
      if (outstandingFutures == null) {
        continue;
      }
      try {
        ApiFutures.allAsList(outstandingFutures.futures).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    allOutstandingFutures.clear();
  }

  /** Publish all the messages in a partition and store the Future's for each publish request. */
  private void publishMessage(String topic, Integer partition, PubsubMessage message) {
    // Get a map containing all futures per partition for the passed in topic.
    Map<Integer, OutstandingFuturesForPartition> outstandingFuturesForTopic =
        allOutstandingFutures.get(topic);
    if (outstandingFuturesForTopic == null) {
      outstandingFuturesForTopic = new HashMap<>();
      allOutstandingFutures.put(topic, outstandingFuturesForTopic);
    }
    // Get the object containing the outstanding futures for this topic and partition..
    OutstandingFuturesForPartition outstandingFutures = outstandingFuturesForTopic.get(partition);
    if (outstandingFutures == null) {
      outstandingFutures = new OutstandingFuturesForPartition();
      outstandingFuturesForTopic.put(partition, outstandingFutures);
    }
    outstandingFutures.futures.add(publisher.publish(message));
  }

  private void createPublisher() {
    ProjectTopicName fullTopic = ProjectTopicName.of(cpsProject, cpsTopic);
    com.google.cloud.pubsub.v1.Publisher.Builder builder =
        com.google.cloud.pubsub.v1.Publisher.newBuilder(fullTopic)
            .setBatchingSettings(
                BatchingSettings.newBuilder()
                    .setDelayThreshold(Duration.ofMillis(maxDelayThresholdMs))
                    .setElementCountThreshold(maxBufferSize)
                    .setRequestByteThreshold(maxBufferBytes)
                    .build())
            .setRetrySettings(
                RetrySettings.newBuilder()
                    // All values that are not configurable come from the defaults for the publisher
                    // client library.
                    .setTotalTimeout(Duration.ofMillis(maxTotalTimeoutMs))
                    .setMaxRpcTimeout(Duration.ofMillis(maxRequestTimeoutMs))
                    .setInitialRetryDelay(Duration.ofMillis(5))
                    .setRetryDelayMultiplier(2)
                    .setMaxRetryDelay(Duration.ofMillis(Long.MAX_VALUE))
                    .setInitialRpcTimeout(Duration.ofSeconds(10))
                    .setRpcTimeoutMultiplier(2)
                    .build());
    try {
      publisher = builder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {}
}

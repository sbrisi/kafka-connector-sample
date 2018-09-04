package com.palmtree.kafka.connector.sink;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.palmtree.kafka.connector.common.ConnectorUtils;
import com.google.pubsub.v1.PubsubMessage;
import java.lang.Runnable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link SampleSinkTask}. */
public class SampleSinkTaskTest {

  private static final String CPS_TOPIC = "the";
  private static final String CPS_PROJECT = "quick";
  private static final String CPS_MIN_BATCH_SIZE1 = "2";
  private static final String CPS_MIN_BATCH_SIZE2 = "9";
  private static final String KAFKA_TOPIC = "brown";
  private static final ByteString KAFKA_MESSAGE1 = ByteString.copyFromUtf8("fox");
  private static final ByteString KAFKA_MESSAGE2 = ByteString.copyFromUtf8("jumps");
  private static final String FIELD_STRING1 = "over";
  private static final String FIELD_STRING2 = "lazy";
  private static final String KAFKA_MESSAGE_KEY = "dog";
  private static final Schema STRING_SCHEMA = SchemaBuilder.string().build();
  private static final Schema BYTE_STRING_SCHEMA =
      SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build();

  private SampleSinkTask task;
  private Map<String, String> props;
  private Publisher publisher;

  private class SpyableFuture<V> implements ApiFuture<V> {
    private V value = null;
    private Throwable exception = null;

    public SpyableFuture(V value) {
      this.value = value;
    }

    public <V> SpyableFuture(Throwable exception) {
      this.exception = exception;
    }

    @Override
    public V get() throws ExecutionException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws ExecutionException {
      return get();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      executor.execute(listener);
    }
  }

  @Before
  public void setup() {
    publisher = mock(Publisher.class, RETURNS_DEEP_STUBS);
    task = new SampleSinkTask(publisher);
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(SampleSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE2);
  }

  /** Tests that an exception is thrown when the schema of the value is not BYTES. */
  @Test
  public void testPutPrimitives() {
    task.start(props);
    SinkRecord record8 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int8(), (byte) 5, -1);
    SinkRecord record16 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int16(), (short) 5, -1);
    SinkRecord record32 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int32(), (int) 5, -1);
    SinkRecord record64 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int64(), (long) 5, -1);
    SinkRecord recordFloat32 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.float32(), (float) 8, -1);
    SinkRecord recordFloat64 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.float64(), (double) 8, -1);
    SinkRecord recordBool =
        new SinkRecord(null, -1, null, null, SchemaBuilder.bool(), true, -1);
    SinkRecord recordString =
        new SinkRecord(null, -1, null, null, SchemaBuilder.string(), "Test put.", -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record8);
    list.add(record16);
    list.add(record32);
    list.add(record64);
    list.add(recordFloat32);
    list.add(recordFloat64);
    list.add(recordBool);
    list.add(recordString);
    task.put(list);
  }

  @Test
  public void testStructSchema() {
    task.start(props);
    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, SchemaBuilder.string()).build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test
  public void testStructSchemaWithOptionalField() {
    task.start(props);

    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, SchemaBuilder.string().optional()).build();

    // With the optional field missing.
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);

    // With the optional field present.
    val.put(FIELD_STRING2, "eagle");
    record = new SinkRecord(null, -1, null, null, schema, val, -1);
    list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test(expected = DataException.class)
  public void testStructSchemaWithMissingField() {
    task.start(props);

    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, SchemaBuilder.string()).build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test(expected = DataException.class)
  public void testStructSchemaWithNestedSchema() {
    task.start(props);

    Schema nestedSchema = SchemaBuilder.struct().build();
    Struct nestedVal = new Struct(nestedSchema);

    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, nestedSchema).build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, nestedVal);
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  @Test
  public void testMapSchema() {
    task.start(props);
    Schema schema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).build();
    Map<String, String> val = new HashMap<>();
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    schema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.bytes()).build();
    record = new SinkRecord(null, -1, null, null, schema, val, -1);
    list.add(record);
    try {
      task.put(list);
    } catch (DataException e) { } // Expected, pass.
  }

  @Test
  public void testArraySchema() {
    task.start(props);
    Schema schema = SchemaBuilder.array(SchemaBuilder.string()).build();
    String[] val = {"Roll", "tide"};
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    schema = SchemaBuilder.array(SchemaBuilder.struct()).build();
    record = new SinkRecord(null, -1, null, null, schema, null, -1);
    list.add(record);
    try {
      task.put(list);
    } catch (DataException e) { } // Expected, pass.
  }

  @Test
  public void testNullSchema() {
    task.start(props);
    String val = "I have no schema";
    SinkRecord record = new SinkRecord(null, -1, null, null, null, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  /**
   * Tests that the correct message is sent to the publisher.
   */
  @Test
  public void testPutWherePublishesAreInvoked() {
    props.put(SampleSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = getSampleRecords();
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();
    assertEquals(requestArgs, getPubsubMessagesFromSampleRecords());
  }

  /**
   * Tests that a call to flush() processes the Futures that were generated by calls to put.
   */
  @Test
  public void testFlushWithNoPublishInPut() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ApiFuture<String> goodFuture = getSuccessfulPublishFuture();
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(goodFuture);
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(2)).publish(any(PubsubMessage.class));
    verify(goodFuture, times(2)).addListener(any(Runnable.class), any(Executor.class));
  }

  /**
   * Tests that if a Future that is being processed in flush() failed with an exception, that an
   * exception is thrown.
   */
  @Test(expected = RuntimeException.class)
  public void testFlushExceptionCase() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ApiFuture<String> badFuture = getFailedPublishFuture();
    when(publisher.publish(any(PubsubMessage.class))).thenReturn(badFuture);
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(1)).publish(any(PubsubMessage.class));
    verify(badFuture, times(1)).addListener(any(Runnable.class), any(Executor.class));
  }

  /**
   * Tests that when requested, Kafka metadata is included in the messages published to Cloud
   * Pub/Sub.
   */
  @Test
  public void testKafkaMetadata() {
    props.put(SampleSinkConnector.PUBLISH_KAFKA_METADATA, "true");
    props.put(SampleSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            1000,
            50000L,
            TimestampType.CREATE_TIME));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            4,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            1001,
            50001L,
            TimestampType.CREATE_TIME));
    task.put(records);
    ArgumentCaptor<PubsubMessage> captor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisher, times(2)).publish(captor.capture());
    List<PubsubMessage> requestArgs = captor.getAllValues();


    List<PubsubMessage> expectedMessages = new ArrayList<>();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY);
    attributes1.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, KAFKA_TOPIC);
    attributes1.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, "4");
    attributes1.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, "1000");
    attributes1.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, "50000");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes1).setData(KAFKA_MESSAGE1).build());
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY);
    attributes2.put(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE, KAFKA_TOPIC);
    attributes2.put(ConnectorUtils.KAFKA_PARTITION_ATTRIBUTE, "4");
    attributes2.put(ConnectorUtils.KAFKA_OFFSET_ATTRIBUTE, "1001");
    attributes2.put(ConnectorUtils.KAFKA_TIMESTAMP_ATTRIBUTE, "50001");
    expectedMessages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes2).setData(KAFKA_MESSAGE2).build());

    assertEquals(requestArgs, expectedMessages);
  }

  /** Get some sample SinkRecords's to use in the tests. */
  private List<SinkRecord> getSampleRecords() {
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            -1));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            -1));
    return records;
  }

  /**
   * Get some PubsubMessage's which correspond to the SinkRecord's created in {@link
   * #getSampleRecords()}.
   */
  private List<PubsubMessage> getPubsubMessagesFromSampleRecords() {
    List<PubsubMessage> messages = new ArrayList<>();
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY);
    messages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(KAFKA_MESSAGE1).build());
    messages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(KAFKA_MESSAGE2).build());
    return messages;
  }

  private ApiFuture<String> getSuccessfulPublishFuture() {
    SpyableFuture<String> future = new SpyableFuture("abcd");
    return spy(future);
  }

  private ApiFuture<String> getFailedPublishFuture() {
    SpyableFuture<String> future = new SpyableFuture(new Exception());
    return spy(future);
  }
}

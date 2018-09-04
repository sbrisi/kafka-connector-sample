package com.palmtree.kafka.connector.source;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.palmtree.kafka.connector.common.ConnectorUtils;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link SampleSourceTask}. */
public class SampleSourceTaskTest {
  private static final Logger log = LoggerFactory.getLogger(SampleSourceTaskTest.class);

  private static final String CPS_PROJECT = "the";
  private static final String CPS_MAX_BATCH_SIZE = "1000";
  private static final String CPS_SUBSCRIPTION = "quick";
  private static final String KAFKA_TOPIC = "brown";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE = "fox";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE = "jumped";
  private static final String KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE = "over";
  private static final String KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE_VALUE = "1234567890";
  private static final String KAFKA_PARTITIONS = "3";
  private static final ByteString CPS_MESSAGE = ByteString.copyFromUtf8("the");
  private static final byte[] KAFKA_VALUE = CPS_MESSAGE.toByteArray();
  private static final String ACK_ID1 = "ackID1";
  private static final String ACK_ID2 = "ackID2";
  private static final String ACK_ID3 = "ackID3";
  private static final String ACK_ID4 = "ackID4";

  private SampleSourceTask task;
  private Map<String, String> props;
  private SampleSubscriber subscriber;

  /**
   * Compare two SourceRecords. This is necessary because the records' values contain a byte[] and
   * the .equals on a SourceRecord does not take this into account.
   */
  public void assertRecordsEqual(SourceRecord sr1, SourceRecord sr2) {
    assertEquals(sr1.key(), sr2.key());
    assertEquals(sr1.keySchema(), sr2.keySchema());
    assertEquals(sr1.valueSchema(), sr2.valueSchema());
    assertEquals(sr1.topic(), sr2.topic());

    if (sr1.valueSchema() == Schema.BYTES_SCHEMA) {
      assertArrayEquals((byte[])sr1.value(), (byte[])sr2.value());
    } else {
      for(Field f : sr1.valueSchema().fields()) {
        if (f.name().equals(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD)) {
          assertArrayEquals(((Struct)sr1.value()).getBytes(f.name()),
                            ((Struct)sr2.value()).getBytes(f.name()));
        } else {
          assertEquals(((Struct)sr1.value()).getString(f.name()),
                       ((Struct)sr2.value()).getString(f.name()));
        }
      }
    }
  }

  @Before
  public void setup() {
    subscriber = mock(SampleSubscriber.class, RETURNS_DEEP_STUBS);
    task = new SampleSourceTask(subscriber);
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(SampleSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG, CPS_MAX_BATCH_SIZE);
    props.put(SampleSourceConnector.CPS_SUBSCRIPTION_CONFIG, CPS_SUBSCRIPTION);
    props.put(SampleSourceConnector.KAFKA_TOPIC_CONFIG, KAFKA_TOPIC);
    props.put(SampleSourceConnector.KAFKA_MESSAGE_KEY_CONFIG, KAFKA_MESSAGE_KEY_ATTRIBUTE);
    props.put(SampleSourceConnector.KAFKA_MESSAGE_TIMESTAMP_CONFIG, KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE);
    props.put(SampleSourceConnector.KAFKA_PARTITIONS_CONFIG, KAFKA_PARTITIONS);
    props.put(
        SampleSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        SampleSourceConnector.PartitionScheme.ROUND_ROBIN.toString());
  }

  /** Tests when no messages are received from the Cloud Pub/Sub PullResponse. */
  @Test
  public void testPollCaseWithNoMessages() throws Exception {
    task.start(props);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    assertEquals(0, task.poll().size());
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
  }

  /**
   * Tests that when ackMessages() succeeds and the subsequent call to poll() has no messages, that
   * the subscriber does not invoke ackMessages because there should be no acks.
   */
  @Test
  public void testPollInRegularCase() throws Exception {
    task.start(props);
    ReceivedMessage rm1 = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<String, String>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm1).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    assertEquals(1, result.size());
    task.commitRecord(result.get(0));
    stubbedPullResponse = PullResponse.newBuilder().build();
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(subscriber.ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    result = task.poll();
    assertEquals(0, result.size());
    result = task.poll();
    assertEquals(0, result.size());
    verify(subscriber, times(1)).ackMessages(any(AcknowledgeRequest.class));
  }


  /**
   * Tests that when a call to ackMessages() fails, that the message is not redelivered to Kafka if
   * the message is received again by Cloud Pub/Sub. Also tests that ack ids are added properly if
   * the ack id has not been seen before.
   */
  @Test
  public void testPollWithDuplicateReceivedMessages() throws Exception {
    task.start(props);
    ReceivedMessage rm1 = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<String, String>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm1).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    assertEquals(1, result.size());
    ReceivedMessage rm2 = createReceivedMessage(ACK_ID2, CPS_MESSAGE, new HashMap<String, String>());
    stubbedPullResponse =
        PullResponse.newBuilder().addReceivedMessages(0, rm1).addReceivedMessages(1, rm2).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    result = task.poll();
    assertEquals(1, result.size());
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do not have an attribute that matches
   * {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithNoMessageKeyAttribute() throws Exception {
    task.start(props);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<String, String>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do have an attribute that matches {@link
   * #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithMessageKeyAttribute() throws Exception {
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do have an attribute that matches {@link
   * #KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE} and {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithMessageTimestampAttribute() throws Exception{
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    attributes.put(KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE, KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE_VALUE);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    SourceRecord expected =
            new SourceRecord(
                    null,
                    null,
                    KAFKA_TOPIC,
                    0,
                    Schema.OPTIONAL_STRING_SCHEMA,
                    KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
                    Schema.BYTES_SCHEMA,
                    KAFKA_VALUE, Long.parseLong(KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE_VALUE));
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests when the message retrieved from Cloud Pub/Sub have several attributes, including
   * one that matches {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithMultipleAttributes() throws Exception {
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    attributes.put("attribute1", "attribute_value1");
    attributes.put("attribute2", "attribute_value2");
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    Schema expectedSchema =
        SchemaBuilder.struct()
            .field(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD, Schema.BYTES_SCHEMA)
            .field("attribute1", Schema.STRING_SCHEMA)
            .field("attribute2", Schema.STRING_SCHEMA)
            .build();
    Struct expectedValue = new Struct(expectedSchema)
                               .put(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD, KAFKA_VALUE)
                               .put("attribute1", "attribute_value1")
                               .put("attribute2", "attribute_value2");
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            expectedSchema,
            expectedValue);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests that the correct partition is assigned when the partition scheme is "hash_key". The test
   * has two cases, one where a key does exist and one where it does not.
   */
  @Test
  public void testPollWithPartitionSchemeHashKey() throws Exception {
    props.put(
        SampleSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        SampleSourceConnector.PartitionScheme.HASH_KEY.toString());
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    ReceivedMessage withoutKey = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<String, String>());
    ReceivedMessage withKey = createReceivedMessage(ACK_ID2, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse =
        PullResponse.newBuilder()
            .addReceivedMessages(0, withKey)
            .addReceivedMessages(1, withoutKey)
            .build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(2, result.size());
    SourceRecord expectedForMessageWithKey =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            Schema.OPTIONAL_STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expectedForMessageWithoutKey =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);

    assertRecordsEqual(expectedForMessageWithKey, result.get(0));
    assertArrayEquals((byte[])expectedForMessageWithoutKey.value(), (byte[])result.get(1).value());
  }

  /** Tests that the correct partition is assigned when the partition scheme is "hash_value". */
  @Test
  public void testPollWithPartitionSchemeHashValue() throws Exception {
    props.put(
        SampleSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        SampleSourceConnector.PartitionScheme.HASH_VALUE.toString());
    task.start(props);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<String, String>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            KAFKA_VALUE.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests that the correct partition is assigned when the partition scheme is "round_robin". The
   * tests makes sure to submit an approrpriate number of messages to poll() so that all partitions
   * in the round robin are hit once.
   */
  @Test
  public void testPollWithPartitionSchemeRoundRobin() throws Exception {
    task.start(props);
    ReceivedMessage rm1 = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<String, String>());
    ReceivedMessage rm2 = createReceivedMessage(ACK_ID2, CPS_MESSAGE, new HashMap<String, String>());
    ReceivedMessage rm3 = createReceivedMessage(ACK_ID3, CPS_MESSAGE, new HashMap<String, String>());
    ReceivedMessage rm4 = createReceivedMessage(ACK_ID4, CPS_MESSAGE, new HashMap<String, String>());
    PullResponse stubbedPullResponse =
        PullResponse.newBuilder()
            .addReceivedMessages(0, rm1)
            .addReceivedMessages(1, rm2)
            .addReceivedMessages(2, rm3)
            .addReceivedMessages(3, rm4)
            .build();
    when(subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any(AcknowledgeRequest.class));
    assertEquals(4, result.size());
    SourceRecord expected1 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected2 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            1,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected3 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            2,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected4 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected1, result.get(0));
    assertRecordsEqual(expected2, result.get(1));
    assertRecordsEqual(expected3, result.get(2));
    assertRecordsEqual(expected4, result.get(3));
  }

  @Test
  public void testPollExceptionCase() throws Exception {
    task.start(props);
    // Could also throw ExecutionException if we wanted to...
    when(subscriber.pull(any(PullRequest.class)).get()).thenThrow(new InterruptedException());
    assertEquals(0, task.poll().size());
  }

  private ReceivedMessage createReceivedMessage(
      String ackId, ByteString data, Map<String, String> attributes) {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(data).putAllAttributes(attributes).build();
    return ReceivedMessage.newBuilder().setAckId(ackId).setMessage(message).build();
  }
}

package com.palmtree.kafka.connector.source;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.palmtree.kafka.connector.common.ConnectorUtils;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import java.io.IOException;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SampleSubscriber} that uses <a href="http://www.grpc.io/">gRPC</a> to pull messages
 * from <a href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>. This class is not
 * thread-safe.
 */
public class SampleGRPCSubscriber implements SampleSubscriber {

  private static final Logger log = LoggerFactory.getLogger(SampleGRPCSubscriber.class);
  private long nextSubscriberResetTime = 0;
  private SubscriberFutureStub subscriber;
  private Random rand = new Random(System.currentTimeMillis());

  SampleGRPCSubscriber() {
    makeSubscriber();
  }

  public ListenableFuture<PullResponse> pull(PullRequest request) {
    if (System.currentTimeMillis() > nextSubscriberResetTime) {
      makeSubscriber();
    }
    return subscriber.pull(request);
  }

  public ListenableFuture<Empty> ackMessages(AcknowledgeRequest request) {
    if (System.currentTimeMillis() > nextSubscriberResetTime) {
      makeSubscriber();
    }
    return subscriber.acknowledge(request);
  }

  private void makeSubscriber() {
    try {
      log.info("Creating subscriber.");
      subscriber = SubscriberGrpc.newFutureStub(ConnectorUtils.getChannel());
      // We change the subscriber every 25 - 35 minutes in order to avoid GOAWAY errors.
      nextSubscriberResetTime =
          System.currentTimeMillis() + rand.nextInt(10 * 60 * 1000) + 25 * 60 * 1000;
    } catch (IOException e) {
      throw new RuntimeException("Could not create subscriber stub; no subscribing can occur.", e);
    }
  }
}

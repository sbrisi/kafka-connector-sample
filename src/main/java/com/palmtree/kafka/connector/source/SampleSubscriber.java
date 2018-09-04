package com.palmtree.kafka.connector.source;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;

/**
 * An interface for clients that want to subscribe to messages from to <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public interface SampleSubscriber {

  public ListenableFuture<PullResponse> pull(PullRequest request);

  public ListenableFuture<Empty> ackMessages(AcknowledgeRequest request);
}

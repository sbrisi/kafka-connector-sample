package com.palmtree.kafka.connector.source;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link SampleSubscriber} that distributes a single subscription in round-robin fashion
 * over a set of {@link SampleGRPCSubscriber}s.
 */
public class SampleRoundRobinSubscriber implements SampleSubscriber {

  private List<SampleSubscriber> subscribers;
  private int currentSubscriberIndex = 0;

  public SampleRoundRobinSubscriber(int subscriberCount) {
    subscribers = new ArrayList<>();
    for (int i = 0; i < subscriberCount; ++i) {
      subscribers.add(new SampleGRPCSubscriber());
    }
  }

  @Override
  public ListenableFuture<PullResponse> pull(PullRequest request) {
    currentSubscriberIndex = (currentSubscriberIndex + 1) % subscribers.size();
    return subscribers.get(currentSubscriberIndex).pull(request);
  }

  @Override
  public ListenableFuture<Empty> ackMessages(AcknowledgeRequest request) {
    currentSubscriberIndex = (currentSubscriberIndex + 1) % subscribers.size();
    return subscribers.get(currentSubscriberIndex).ackMessages(request);
  }
}

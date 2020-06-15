package com.manju.gcp.pubsub;

import java.util.ArrayList;
import java.util.List;
import org.threeten.bp.Duration;
import com.google.api.client.util.Throwables;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;

public class StreamingService {
  
  private List<Subscriber> subscribers = new ArrayList<>();
  
  private MessageReceiverImpl messageReceiverImpl = new MessageReceiverImpl();
  
  public StreamingService(String projectId, String subscriptionId, int threadsCount) {
    createSubscriber(projectId, subscriptionId, threadsCount);
  }
  
  /**
   * Method to create number of subscribers based on the subscribers count.
   */
  private void createSubscriber(String projectId, String subscriptionId, int subscribersCount) {
    System.out.println("Starting subscriber-" + subscriptionId);
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
    ExecutorProvider executorProvider =
            InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(3).build();
    FlowControlSettings flowControlSettings =
            FlowControlSettings.newBuilder()
                    .setMaxOutstandingElementCount(10_000L)
                    .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
                    .build();
    for(int i=0; i < subscribersCount; i++) {
        Subscriber s =  Subscriber.newBuilder(subscriptionName, messageReceiverImpl).setExecutorProvider(executorProvider)
                .setFlowControlSettings(flowControlSettings).setMaxAckExtensionPeriod(Duration.ofMinutes(10)).build();
        s.addListener(new Subscriber.Listener() {
            @Override
            public void failed(Subscriber.State from, Throwable failure) {
                System.err.println("Exception thrown in Subscriber: " + failure.toString());
                System.err.println("Subscriber state: " + from.toString());
                Throwables.propagate(failure);
            }
        }, MoreExecutors.directExecutor());
        subscribers.add(s);
    }
  }
  
  /**
   * Method to close the subscribers.
   */
  public void stopStream() {
    messageReceiverImpl.shutdown();
    subscribers.forEach(Subscriber::stopAsync);
    subscribers.forEach(Subscriber::awaitTerminated);
  }
  
  /**
   * Method to start the subscribers to receiver messages from pub/sub.
   */
  public void startStream() {
    subscribers.forEach(Subscriber::startAsync);
  }

}

package com.manju.gcp.pubsub;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.threeten.bp.Duration;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubService {

  private Publisher publisher;

  /**
   * Constructor to initialise the publisher connection.
   */
  public PubSubService(String gcpProjectId, String topicName) {
      try {
          ProjectTopicName topic = ProjectTopicName.of(gcpProjectId, topicName);
          RetrySettings retrySettings =
                  RetrySettings.newBuilder()   //Retry settings to retry in case of any error in publishing the message.
                          .setInitialRetryDelay(Duration.ofMillis(5))
                          .setRetryDelayMultiplier(2.0)
                          .setMaxRetryDelay(Duration.ofSeconds(600))
                          .setTotalTimeout(Duration.ofSeconds(10))
                          .setInitialRpcTimeout(Duration.ofSeconds(10))
                          .setMaxRpcTimeout(Duration.ofSeconds(10))
                          .setMaxAttempts(5)
                          .build();
          BatchingSettings batchingSettings = BatchingSettings.newBuilder() //Batch settings to batch the messages
                  .setElementCountThreshold(20L)
                  .setRequestByteThreshold(10000L)
                  .setDelayThreshold(Duration.ofMillis(5))
                  .build();
          publisher = Publisher.newBuilder(topic).setBatchingSettings(batchingSettings).
                  setRetrySettings(retrySettings)
                  .build();
          System.out.println("Initialized gcp pubsub publisher.");
      } catch (IOException io) {
          System.out.println("Error occurred while creating the gcp pubsub publisher -> " + io.getLocalizedMessage());
          throw new RuntimeException(io);
      }
  }

  /**
   * Method to publish message into google cloud pub/sub in an asynchronous way.
   *
   * @param message -- Message to be published to pub/sub in an asynchronous way.
   */
  public void publishMessage(String message) {
      if(!StringUtils.isEmpty(message)) {
          try {
              System.out.println("Posting message into PubSub->" + message);
              ByteString data = ByteString.copyFromUtf8(message);
              PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
              ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
              ApiFutures.addCallback(
                      messageIdFuture,
                      new ApiFutureCallback<String>() {
                          public void onSuccess(String messageId) {
                              System.out.println("Saved record. Value -> " +  message);
                          }
                          public void onFailure(Throwable t) {
                              System.err.println("Error while saving record to pubsub and exception is ->" + t.getLocalizedMessage());
                          }
                      }, MoreExecutors.directExecutor()
              );
          } catch(Exception ex) {
              System.out.println("Error occurred while publishing message into PubSub-> " + ex.getLocalizedMessage());
              ex.printStackTrace();
              throw new RuntimeException(ex);
          }
      }
  }

  /**
   * Method to shutdown pub/sub publisher.
   */
  public void shutdown() {
      try {
          System.out.println("Shutting down pub sub publisher.");
          publisher.shutdown();
          publisher.awaitTermination(1, TimeUnit.MINUTES);
      } catch(Exception ex) {
          System.err.println("Error occurred while shutting down the publisher -> " + ex.getLocalizedMessage());
      }
  }
}

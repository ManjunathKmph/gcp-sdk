package com.manju.gcp.pubsub;

import java.util.concurrent.TimeUnit;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

public class MessageReceiverImpl implements MessageReceiver {
  
  private WorkerPool workerPool;
  
  public MessageReceiverImpl() {
    workerPool = new WorkerPool(100, 5, 5, 60, TimeUnit.SECONDS);
  }
  
  /**
   * Method to receive message from pub/sub subscription and then submit it into worker queue.
   */
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    workerPool.submit(new MessageProcessing(message, consumer));
  }
  
  /**
   * Method to close the queue.
   */
  public void shutdown() {
    System.out.println("Shutting down message receiver worker queue");
    workerPool.close();
  }

}

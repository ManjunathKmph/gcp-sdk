package com.manju.gcp.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;

public class MessageProcessing implements Runnable {
  
  private PubsubMessage pubsubMessage;

  private AckReplyConsumer consumer;

  public MessageProcessing(PubsubMessage pubsubMessage, AckReplyConsumer consumer) {
      this.pubsubMessage = pubsubMessage;
      this.consumer = consumer;
  }
  
  public void run() {
    try {
      System.out.println("Got Message from queue:" + pubsubMessage.getData().toStringUtf8());
      consumer.ack();
    } catch(Exception ex) {
      consumer.nack();
      System.err.println("Error in processing message from pubsub->" + pubsubMessage.getData().toStringUtf8() + ", Error:" + ex.getLocalizedMessage());
      ex.printStackTrace();
    }
  }
  
}

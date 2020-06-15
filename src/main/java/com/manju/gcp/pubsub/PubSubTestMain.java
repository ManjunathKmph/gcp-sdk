package com.manju.gcp.pubsub;

import java.util.concurrent.CountDownLatch;

public class PubSubTestMain {

  public static void main(String[] args) {
    
    //Note:- Before running this class, create pub/sub topic and subscription in google cloud.
    
    //Create Publisher and publish some messages into pub/sub topic
    PubSubService pubSubService = new PubSubService("", "");
    for(int i =0; i < 1000; i++) {
      pubSubService.publishMessage("Message - " + i);
    }
    pubSubService.shutdown();
    
    //Create Subscribers to receive the published messages.
    final CountDownLatch latch = new CountDownLatch(1);
    StreamingService streamingService = new StreamingService("", "", 30);
    
    //Registering a shutdown hook so that main thread won't be killed before subscribers are done with messages.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutdown Hook Called");
      streamingService.stopStream();
      System.out.println("Shutdown Hook Finished");
      latch.countDown();
    }));
  
    streamingService.startStream();
    System.out.println("Application Started");
  
    try {
        latch.await();
    }  catch (Throwable e) {
        System.exit(1);
    }
  }

}

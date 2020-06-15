package com.manju.gcp.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class WorkerPool {

    private static final Integer QUEUE_SIZE_DEFAULT = 1000;
    private static final Integer CORE_SIZE_DEFAULT = 30;
    private static final Integer MAX_SIZE_DEFAULT = 35;
    private static final Integer KEEP_ALIVE_TIME_DEFAULT = 1;
    private static final TimeUnit TIME_UNIT_DEFAULT = TimeUnit.MINUTES;

    private BlockingQueue<Runnable> workQueue;
    private List<Future<?>> futureList = new ArrayList<>();
    private ExecutorService threadPool;
    
    public static class ThreadFactoryImpl implements ThreadFactory {

        private String threadPrefix;

        public ThreadFactoryImpl(String threadPrefix) {
            this.threadPrefix = threadPrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, threadPrefix);
        }

    }

    public void init(Integer queueSize, Integer coreSize, Integer maxSize, Integer keepAliveTime, TimeUnit timeUnit) {
        this.workQueue = new ArrayBlockingQueue<>(QUEUE_SIZE_DEFAULT);
        this.threadPool = new ThreadPoolExecutor(CORE_SIZE_DEFAULT,
                MAX_SIZE_DEFAULT,
                KEEP_ALIVE_TIME_DEFAULT,
                TIME_UNIT_DEFAULT,
                workQueue,
                new ThreadFactoryImpl("worker-pool-thread-"));
    }

    public WorkerPool() {
        init(QUEUE_SIZE_DEFAULT, CORE_SIZE_DEFAULT, MAX_SIZE_DEFAULT, KEEP_ALIVE_TIME_DEFAULT, TIME_UNIT_DEFAULT);
    }

    public WorkerPool(int queueSize, int coreSize, int maxSize, int keepAliveTime, TimeUnit timeUnit) {
        init(queueSize, coreSize, maxSize, keepAliveTime, timeUnit);
    }
    
    public void submit(Runnable runnable) {
        while (workQueue.size() > 995) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                System.err.println("Error in sleep " + e.getLocalizedMessage());
            }
        }
        futureList.add(threadPool.submit(runnable));
    }
    
    public void close() {
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Error in closing thread pool" + e.getLocalizedMessage());
        }
    }
}

package com.heibaiying.springboot.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaInit {
    // 定时任务线程池+postConstruct
    @Autowired
    private GroupQueue groupQueue;

    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1 , r -> new Thread(r, "KafkaInit"));

    @PostConstruct
    public void startBackgroundTasks() {
        executorService.scheduleWithFixedDelay(groupQueue, 5, 5, TimeUnit.SECONDS);
    }
}

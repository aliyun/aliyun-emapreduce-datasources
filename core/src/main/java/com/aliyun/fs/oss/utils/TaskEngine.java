package com.aliyun.fs.oss.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class TaskEngine {
    private ExecutorService executorService;

    private CountDownLatch unCompletedTask;

    private List<Task> taskList;

    private Map<String,Object> resultMap = new HashMap<String,Object>();

    public TaskEngine(List<Task> taskList, int coreSize, int maxSize) {
        this.taskList = taskList;
        unCompletedTask = new CountDownLatch(taskList.size());
        executorService = new ThreadPoolExecutor(coreSize, maxSize, 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());
    }

    public void reportCompleted() {
        unCompletedTask.countDown();
    }

    public synchronized void registerResponse(String key,Object value) {
        resultMap.put(key,value);
    }

    public Map<String, Object> getResultMap() throws InterruptedException {
        unCompletedTask.await();
        return resultMap;
    }

    public void executeTask() {
        for(Task task : taskList) {
            task.setTaskEngine(this);
            executorService.execute(task);
        }
    }

    public void shutdown() {
        this.executorService.shutdown();
    }
}
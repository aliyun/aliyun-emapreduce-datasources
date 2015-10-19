/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
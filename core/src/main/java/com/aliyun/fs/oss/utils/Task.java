package com.aliyun.fs.oss.utils;

public abstract class Task implements Runnable {

    private TaskEngine taskEngine;

    protected Object response;

    protected String uuid = this.toString();

    public void setTaskEngine(TaskEngine taskEngine) {
        this.taskEngine = taskEngine;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    @Override
    public void run() {
        execute(taskEngine);
        taskEngine.registerResponse(uuid, response);
        taskEngine.reportCompleted();
    }

    public abstract void execute(TaskEngine engineRef);
}

package com.migu.schedule.info;

public class Task
{
    /**
     * 任务Id
     */
    private int taskId;
    
    /**
     * 资源消耗率
     */
    private int consumption;
    
    public Task(int taskId, int consumption)
    {
        this.taskId = taskId;
        this.consumption = consumption;
    }
    
    public int getTaskId()
    {
        return taskId;
    }
    
    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
    }
    
    public int getConsumption()
    {
        return consumption;
    }
    
    public void setConsumption(int consumption)
    {
        this.consumption = consumption;
    }
}

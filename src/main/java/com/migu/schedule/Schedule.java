package com.migu.schedule;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.Task;
import com.migu.schedule.info.TaskInfo;


/*
*类名和方法不能修改
 */
public class Schedule {
    /**
     * 注册节点Set
     */
    public static HashSet<Integer> registerNodeSet = new HashSet<Integer>();
    
    /**
     * 任务挂起队列
     */
    public static Queue<Task> waitQueue = new LinkedList<Task>();
    
    /**
     * 任务分配数组
     */
    public static Map<Integer, LinkedList<Task>> RuningTask = new HashMap<Integer, LinkedList<Task>>();
    
    public int init() {
        
        registerNodeSet = new HashSet<Integer>();
        waitQueue = new LinkedList<Task>();
        RuningTask = new HashMap<Integer, LinkedList<Task>>();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        if (registerNodeSet.contains(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        registerNodeSet.add(nodeId);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        if (!registerNodeSet.contains(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        LinkedList<Task> taskList = RuningTask.get(nodeId);
        if(null != taskList && taskList.size()>0)
        {
            waitQueue.addAll(taskList);
        }
        registerNodeSet.remove(nodeId);
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        for (Task task : waitQueue)
        {
            if (taskId == task.getTaskId())
            {
                return ReturnCodeKeys.E010;
            }
        }
        
        Task task = new Task(taskId, consumption);
        waitQueue.add(task);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        for (Task task : waitQueue)
        {
            if (taskId == task.getTaskId())
            {
                waitQueue.remove(task);
                return ReturnCodeKeys.E011;
            }
        }
        for (LinkedList<Task> tasks : RuningTask.values())
        {
            for (Task task : tasks)
            {
                if (taskId == task.getTaskId())
                {
                    tasks.remove(task);
                    return ReturnCodeKeys.E011;
                }
            }
        }
        return ReturnCodeKeys.E012;
    }


    public int scheduleTask(int threshold) {
        int taskNum = 50/registerNodeSet.size();
        if (waitQueue.size() < taskNum && waitQueue.size() > 0)
        {
            taskNum = waitQueue.size();
        }
        List<Task> taskPlanList = new ArrayList<Task>();
        for (int i = 0; i < taskNum; i++)
        {
            Task task = waitQueue.poll();
            taskPlanList.add(task);
        }
        
        
        return ReturnCodeKeys.E000;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        for (Task task : waitQueue)
        {
            TaskInfo taskinfo = new TaskInfo();
            taskinfo.setTaskId(task.getTaskId());
            taskinfo.setNodeId(-1);
            tasks.add(taskinfo);
        }
        for (LinkedList<Task> taskList : RuningTask.values())
        {
            for (Task task : taskList)
            {
                TaskInfo taskinfo = new TaskInfo();
                taskinfo.setTaskId(task.getTaskId());
                taskinfo.setNodeId(-1);
                tasks.add(taskinfo);
            }
        }
        if (tasks == null || tasks.size() == 0)
        {
            return ReturnCodeKeys.E016;
        }
        return ReturnCodeKeys.E015;
    }

}

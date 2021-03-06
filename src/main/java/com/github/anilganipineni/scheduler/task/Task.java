/**
 * Copyright (C) Anil Ganipineni
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.anilganipineni.scheduler.task;

import java.util.Map;

import com.github.anilganipineni.scheduler.SchedulerImpl;
import com.github.anilganipineni.scheduler.StringUtils;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.schedule.Clock;
import com.github.anilganipineni.scheduler.schedule.Schedule;
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.ExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;

/**
 * @author akganipineni
 */
public abstract class Task implements ExecutionHandler {
	private String name;
    private FailureHandler failureHandler;
    private DeadExecutionHandler deadExecutionHandler;
    
    private Schedule schedule;
    private String instance;
    private Map<String, Object> data;
    /**
     * @param name
     * @param failureHandler
     * @param deadExecutionHandler
     */
    public Task(String name, FailureHandler failureHandler, DeadExecutionHandler deadExecutionHandler) {
        this.name = name;
        this.failureHandler = failureHandler;
        this.deadExecutionHandler = deadExecutionHandler;
    }
    /**
     * @param name
     * @param failureHandler
     * @param deadExecutionHandler
     * @param schedule
     * @param instance
     * @param data
     */
    public Task(String name, FailureHandler failureHandler, DeadExecutionHandler deadExecutionHandler, Schedule schedule, String instance, Map<String, Object> data) {
        this.name = name;
        this.failureHandler = failureHandler;
        this.deadExecutionHandler = deadExecutionHandler;
        
        this.schedule = schedule;
        this.instance = instance;
        this.data = data;
    }
    /**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @return the instance
	 */
	public String getInstance() {
		return instance;
	}
	/**
	 * @return the data
	 */
	public Object getData() {
		return data;
	}
	/**
	 * @return the schedule
	 */
	public Schedule getSchedule() {
		return schedule;
	}
	/**
	 * @return the failureHandler
	 */
	public FailureHandler getFailureHandler() {
		return failureHandler;
	}
	/**
	 * @return the deadExecutionHandler
	 */
	public DeadExecutionHandler getDeadExecutionHandler() {
		return deadExecutionHandler;
	}
	/**
	 * @param id
	 * @return
	 */
	public ScheduledTasks instance(String id) {
        return new ScheduledTasks(null, this.name, id);
    }
	/**
	 * @param id
	 * @param data
	 * @return
	 */
	public ScheduledTasks instance(String id, Map<String, Object> data) {
        return new ScheduledTasks(null, this.name, id, StringUtils.convertMap2String(data));
    }
	/**
	 * @param id
	 * @param data
	 * @return
	 */
	public ScheduledTasks instance(String id, String data) {
        return new ScheduledTasks(null, this.name, id, data);
    }
	/**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Task = " + getName();
    }
	/**
	 * @see com.github.anilganipineni.scheduler.task.handler.ExecutionHandler#onStartup(com.github.anilganipineni.scheduler.SchedulerImpl,
	 *      com.github.anilganipineni.scheduler.schedule.Clock)
	 */
	@Override
	public void onStartup(SchedulerImpl scheduler, Clock clock) {
    	if(schedule != null) {
    		scheduler.schedule(instance(instance, StringUtils.convertMap2String(this.data)), schedule.getInitialExecutionTime(clock.now()));
    	}
    }
}

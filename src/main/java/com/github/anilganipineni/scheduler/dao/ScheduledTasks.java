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
package com.github.anilganipineni.scheduler.dao;

import java.time.Instant;
import java.util.Objects;

/**
 * @author akganipineni
 */
@com.datastax.driver.mapping.annotations.Table(name = ScheduledTasks.TABLE_NAME)
public final class ScheduledTasks {
	public static final String TABLE_NAME	= "scheduled_tasks";
    private String taskName;
    private String id;
    private Object taskData;
    public  Instant executionTime;
    public  boolean picked;
    public  String pickedBy;
    public int consecutiveFailures;
    public  Instant lastHeartbeat;
    public  long version;
    public  Instant lastFailure;
    public  Instant lastSuccess;
    /**
	 * @param taskName
	 * @param id
	 */
	public ScheduledTasks(String taskName, String id) {
		this.taskName = taskName;
		this.id = id;
	}
    /**
	 * @param executionTime
	 * @param taskName
	 * @param id
	 */
	public ScheduledTasks(Instant executionTime, String taskName, String id) {
		this.executionTime = executionTime;
		this.taskName = taskName;
		this.id = id;
	}
    /**
	 * @param executionTime
	 * @param taskName
	 * @param id
	 * @param taskData
	 */
	public ScheduledTasks(Instant executionTime, String taskName, String id, Object taskData) {
		this.executionTime = executionTime;
		this.taskName = taskName;
		this.id = id;
		this.taskData = taskData;
	}

	public ScheduledTasks(Instant executionTime, String taskName, String id, Object taskData, boolean picked, String pickedBy,
                     Instant lastSuccess, Instant lastFailure, int consecutiveFailures, Instant lastHeartbeat, long version) {
		this.executionTime = executionTime;
		this.taskName = taskName;
		this.id = id;
		this.taskData = taskData;
        this.picked = picked;
        this.pickedBy = pickedBy;
        this.lastFailure = lastFailure;
        this.lastSuccess = lastSuccess;
        this.consecutiveFailures = consecutiveFailures;
        this.lastHeartbeat = lastHeartbeat;
        this.version = version;
    }
	/**
	 * @return the taskName
	 */
	public String getTaskName() {
		return taskName;
	}
	/**
	 * @param taskName the taskName to set
	 */
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @return the taskData
	 */
	public Object getTaskData() {
		return taskData;
	}
	/**
	 * @param taskData the taskData to set
	 */
	public void setTaskData(Object taskData) {
		this.taskData = taskData;
	}
	/**
	 * @return
	 */
	public String getTaskAndInstance() {
        return taskName + "_" + id;
    }
	/**
	 * @return
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "id")
	public Instant getExecutionTime() {
        return executionTime;
    }
    /**
     * @return
     */
    public boolean isPicked() {
        return picked;
    }
    /**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {
        if (this == o) return true;
        
        if (o == null || getClass() != o.getClass()) return false;
        
        ScheduledTasks execution = (ScheduledTasks) o;
        
        return Objects.equals(executionTime, execution.executionTime) && Objects.equals(taskName, execution.taskName) && Objects.equals(id, execution.id);
	}
    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        int result = taskName.hashCode();
        result = 31 * result + id.hashCode();
        return Objects.hash(executionTime, result);
    }
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ScheduledTasks: " +
                "task=" + taskName +
                ", id=" + id +
                ", executionTime=" + executionTime +
                ", picked=" + picked +
                ", pickedBy=" + pickedBy +
                ", lastHeartbeat=" + lastHeartbeat +
                ", version=" + version;
    }
}

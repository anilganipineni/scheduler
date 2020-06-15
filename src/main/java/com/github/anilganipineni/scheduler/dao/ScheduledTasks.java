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
    private String taskId;
    private int consecutiveFailures;
    private  Instant executionTime;
    private  Instant lastFailure;
    private  Instant lastHeartbeat;
    private  Instant lastSuccess;
    private  boolean picked;
    private  String pickedBy;
    private String taskData;
    private  int version;
    /**
	 * Default constructor
	 */
	public ScheduledTasks() {
		/* NO-OP */
	}
    /**
	 * @param taskName
	 * @param taskId
	 */
	public ScheduledTasks(String taskName, String taskId) {
		this.taskName = taskName;
		this.taskId = taskId;
	}
    /**
	 * @param executionTime
	 * @param taskName
	 * @param taskId
	 */
	public ScheduledTasks(Instant executionTime, String taskName, String taskId) {
		this.executionTime = executionTime;
		this.taskName = taskName;
		this.taskId = taskId;
	}
    /**
	 * @param executionTime
	 * @param taskName
	 * @param taskId
	 * @param taskData
	 */
	public ScheduledTasks(Instant executionTime, String taskName, String taskId, String taskData) {
		this.executionTime = executionTime;
		this.taskName = taskName;
		this.taskId = taskId;
		this.taskData = taskData;
	}

	public ScheduledTasks(Instant executionTime, String taskName, String taskId, String taskData, boolean picked, String pickedBy,
                     Instant lastSuccess, Instant lastFailure, int consecutiveFailures, Instant lastHeartbeat, int version) {
		this.executionTime = executionTime;
		this.taskName = taskName;
		this.taskId = taskId;
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
	@com.datastax.driver.mapping.annotations.Column(name = "task_name")
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
	 * @return the taskId
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "task_id")
	public String getTaskId() {
		return taskId;
	}
	/**
	 * @param taskId the taskId to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	/**
	 * @return the consecutiveFailures
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "consecutive_failures")
	public int getConsecutiveFailures() {
		return consecutiveFailures;
	}
	/**
	 * @param consecutiveFailures the consecutiveFailures to set
	 */
	public void setConsecutiveFailures(int consecutiveFailures) {
		this.consecutiveFailures = consecutiveFailures;
	}
	/**
	 * @return the executionTime
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "execution_time")
	public Instant getExecutionTime() {
		return executionTime;
	}
	/**
	 * @param executionTime the executionTime to set
	 */
	public void setExecutionTime(Instant executionTime) {
		this.executionTime = executionTime;
	}
	/**
	 * @return the lastFailure
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "last_failure")
	public Instant getLastFailure() {
		return lastFailure;
	}
	/**
	 * @param lastFailure the lastFailure to set
	 */
	public void setLastFailure(Instant lastFailure) {
		this.lastFailure = lastFailure;
	}
	/**
	 * @return the lastHeartbeat
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "last_heartbeat")
	public Instant getLastHeartbeat() {
		return lastHeartbeat;
	}
	/**
	 * @param lastHeartbeat the lastHeartbeat to set
	 */
	public void setLastHeartbeat(Instant lastHeartbeat) {
		this.lastHeartbeat = lastHeartbeat;
	}
	/**
	 * @return the lastSuccess
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "last_success")
	public Instant getLastSuccess() {
		return lastSuccess;
	}
	/**
	 * @return the picked
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "picked")
	public boolean isPicked() {
		return picked;
	}
	/**
	 * @param picked the picked to set
	 */
	public void setPicked(boolean picked) {
		this.picked = picked;
	}
	/**
	 * @return the pickedBy
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "picked_by")
	public String getPickedBy() {
		return pickedBy;
	}
	/**
	 * @param pickedBy the pickedBy to set
	 */
	public void setPickedBy(String pickedBy) {
		this.pickedBy = pickedBy;
	}
	/**
	 * @param lastSuccess the lastSuccess to set
	 */
	public void setLastSuccess(Instant lastSuccess) {
		this.lastSuccess = lastSuccess;
	}
	/**
	 * @return the taskData
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "task_data")
	public String getTaskData() {
		return taskData;
	}
	/**
	 * @param taskData the taskData to set
	 */
	public void setTaskData(String taskData) {
		this.taskData = taskData;
	}
	/**
	 * @return the version
	 */
	@com.datastax.driver.mapping.annotations.Column(name = "version")
	public int getVersion() {
		return version;
	}
	/**
	 * @param version the version to set
	 */
	public void setVersion(int version) {
		this.version = version;
	}
    /**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {
        if (this == o) return true;
        
        if (o == null || getClass() != o.getClass()) return false;
        
        ScheduledTasks execution = (ScheduledTasks) o;
        
        return Objects.equals(executionTime, execution.executionTime) && Objects.equals(taskName, execution.taskName) && Objects.equals(taskId, execution.taskId);
	}
    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        int result = taskName.hashCode();
        result = 31 * result + taskId.hashCode();
        return Objects.hash(executionTime, result);
    }
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ScheduledTasks: " +
                "task=" + taskName +
                ", taskId=" + taskId +
                ", executionTime=" + executionTime +
                ", picked=" + picked +
                ", pickedBy=" + pickedBy +
                ", lastHeartbeat=" + lastHeartbeat +
                ", version=" + version;
    }
}

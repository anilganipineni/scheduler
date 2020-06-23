package com.github.anilganipineni.scheduler;

import java.time.Instant;

/**
 * @author akganipineni
 */
public class EventContext {
    private final EventType eventType;
    private final String taskName;
    private final String id;
    private final Object taskData;
    private final Instant executionTime;
    /**
     * @param eventType
     * @param taskName
     * @param id
     * @param taskData
     * @param executionTime
     */
    public EventContext(EventType eventType, String taskName, String id, Object taskData, Instant executionTime) {
        this.eventType = eventType;
		this.taskName = taskName;
		this.id = id;
		this.taskData = taskData;
        this.executionTime = executionTime;
    }
	/**
	 * @return the eventType
	 */
	public EventType getEventType() {
		return eventType;
	}
	/**
	 * @return the taskName
	 */
	public String getTaskName() {
		return taskName;
	}
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @return the taskData
	 */
	public Object getTaskData() {
		return taskData;
	}
	/**
	 * @return the executionTime
	 */
	public Instant getExecutionTime() {
		return executionTime;
	}


}

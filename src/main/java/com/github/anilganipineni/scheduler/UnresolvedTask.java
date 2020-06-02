package com.github.anilganipineni.scheduler;

import java.time.Instant;

/**
 * @author akganipineni
 */
public class UnresolvedTask {
    private final String taskName;
    private final Instant firstUnresolved;
    /**
     * @param taskName
     * @param clock
     */
    public UnresolvedTask(String taskName, Clock clock) {
        this.taskName = taskName;
        firstUnresolved = clock.now();
    }
	/**
	 * @return the taskName
	 */
	public String getTaskName() {
		return taskName;
	}
	/**
	 * @return the firstUnresolved
	 */
	public Instant getFirstUnresolved() {
		return firstUnresolved;
	}
}

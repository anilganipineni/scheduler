package com.github.anilganipineni.scheduler.task.handler;

import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.task.CompletionHandler;
import com.github.anilganipineni.scheduler.task.helper.ExecutionComplete;
import com.github.anilganipineni.scheduler.task.helper.ExecutionOperations;
import com.github.anilganipineni.scheduler.task.schedule.Schedule;

/**
 * @author akganipineni
 */
public class OnCompleteReschedule implements CompletionHandler {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(OnCompleteReschedule.class);
    private final Schedule schedule;
	/**
	 * @param schedule
	 */
	public OnCompleteReschedule(Schedule schedule) {
		this.schedule = schedule;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.task.CompletionHandler#complete(com.github.anilganipineni.scheduler.task.helper.ExecutionComplete,
	 *      com.github.anilganipineni.scheduler.task.helper.ExecutionOperations)
	 */
	@Override
	public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
        Instant nextExecution = schedule.getNextExecutionTime(executionComplete);
        logger.debug("Rescheduling task {} to {}", executionComplete.getExecution(), nextExecution);
        executionOperations.reschedule(executionComplete, nextExecution);	
	}
}

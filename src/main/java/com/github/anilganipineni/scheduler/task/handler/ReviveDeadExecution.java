package com.github.anilganipineni.scheduler.task.handler;

import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.ExecutionOperations;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;

/**
 * @author akganipineni
 */
public class ReviveDeadExecution implements DeadExecutionHandler {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(ReviveDeadExecution.class);

    @Override
    public void deadExecution(ScheduledTasks execution, ExecutionOperations executionOperations) {
        final Instant now = Instant.now();
        logger.info("Reviving dead execution: " + execution + " to " + now);
        executionOperations.reschedule(new ExecutionComplete(execution, now, now, ExecutionComplete.Result.FAILED, null), now);
    }

}

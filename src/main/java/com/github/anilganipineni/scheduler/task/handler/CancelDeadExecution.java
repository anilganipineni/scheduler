package com.github.anilganipineni.scheduler.task.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.ExecutionOperations;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;

/**
 * @author akganipineni
 */
public class CancelDeadExecution implements DeadExecutionHandler {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(CancelDeadExecution.class);

    @Override
    public void deadExecution(ScheduledTasks execution, ExecutionOperations executionOperations) {
        logger.warn("Cancelling dead execution: " + execution);
        executionOperations.stop();
    }

}

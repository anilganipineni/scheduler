package com.github.anilganipineni.scheduler.task.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.ExecutionOperations;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;

/**
 * @author akganipineni
 */
public class CancelDeadExecution implements DeadExecutionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReviveDeadExecution.class);

    @Override
    public void deadExecution(ScheduledTasks execution, ExecutionOperations executionOperations) {
        LOG.warn("Cancelling dead execution: " + execution);
        executionOperations.stop();
    }

}

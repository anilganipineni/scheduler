package com.github.anilganipineni.scheduler.task.handler;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.ExecutionOperations;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;

/**
 * @author akganipineni
 */
public class ReviveDeadExecution implements DeadExecutionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReviveDeadExecution.class);

    @Override
    public void deadExecution(ScheduledTasks execution, ExecutionOperations executionOperations) {
        final Instant now = Instant.now();
        LOG.info("Reviving dead execution: " + execution + " to " + now);
        executionOperations.reschedule(new ExecutionComplete(execution, now, now, ExecutionComplete.Result.FAILED, null), now);
    }

}

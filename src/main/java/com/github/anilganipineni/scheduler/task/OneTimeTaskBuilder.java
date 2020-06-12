package com.github.anilganipineni.scheduler.task;

import java.time.Duration;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;
import com.github.anilganipineni.scheduler.task.handler.OnFailureRetryLater;
import com.github.anilganipineni.scheduler.task.handler.VoidExecutionHandler;
import com.github.anilganipineni.scheduler.task.helper.ExecutionContext;

/**
 * @author akganipineni
 */
public class OneTimeTaskBuilder {
    private static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofMinutes(5);
    private final String name;
    private FailureHandler onFailure;
    private DeadExecutionHandler onDeadExecution;

    public OneTimeTaskBuilder(String name) {
        this.name = name;
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution();
        this.onFailure = new OnFailureRetryLater(DEFAULT_RETRY_INTERVAL);
    }

    public OneTimeTaskBuilder onFailureRetryLater() {
        this.onFailure = new OnFailureRetryLater(DEFAULT_RETRY_INTERVAL);
        return this;
    }

    public OneTimeTaskBuilder onDeadExecutionRevive() {
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution();
        return this;
    }

    public OneTimeTaskBuilder onFailure(FailureHandler failureHandler) {
        this.onFailure = failureHandler;
        return this;
    }

    public OneTimeTaskBuilder onDeadExecution(DeadExecutionHandler deadExecutionHandler) {
        this.onDeadExecution = deadExecutionHandler;
        return this;
    }

    public OneTimeTask execute(VoidExecutionHandler executionHandler) {
        return new OneTimeTask(name, onFailure, onDeadExecution) {
            @Override
            public void executeActual(ScheduledTasks taskInstance, ExecutionContext executionContext) {
                executionHandler.execute(taskInstance, executionContext);
            }
        };
    }


}

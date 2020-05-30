package com.github.anilganipineni.scheduler.task.helper;

import java.time.Duration;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.ExecutionContext;
import com.github.anilganipineni.scheduler.task.FailureHandler;
import com.github.anilganipineni.scheduler.task.VoidExecutionHandler;

/**
 * @author akganipineni
 */
public class OneTimeTaskBuilder<T> {
    private static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofMinutes(5);
    private final String name;
    private Class<T> dataClass;
    private FailureHandler<T> onFailure;
    private DeadExecutionHandler<T> onDeadExecution;

    public OneTimeTaskBuilder(String name, Class<T> dataClass) {
        this.name = name;
        this.dataClass = dataClass;
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution<>();
        this.onFailure = new FailureHandler.OnFailureRetryLater<>(DEFAULT_RETRY_INTERVAL);
    }

    public OneTimeTaskBuilder<T> onFailureRetryLater() {
        this.onFailure = new FailureHandler.OnFailureRetryLater<>(DEFAULT_RETRY_INTERVAL);
        return this;
    }

    public OneTimeTaskBuilder<T> onDeadExecutionRevive() {
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution<>();
        return this;
    }

    public OneTimeTaskBuilder<T> onFailure(FailureHandler<T> failureHandler) {
        this.onFailure = failureHandler;
        return this;
    }

    public OneTimeTaskBuilder<T> onDeadExecution(DeadExecutionHandler<T> deadExecutionHandler) {
        this.onDeadExecution = deadExecutionHandler;
        return this;
    }

    public OneTimeTask<T> execute(VoidExecutionHandler<T> executionHandler) {
        return new OneTimeTask<T>(name, dataClass, onFailure, onDeadExecution) {
            @Override
            public void executeOnce(ScheduledTasks taskInstance, ExecutionContext executionContext) {
                executionHandler.execute(taskInstance, executionContext);
            }
        };
    }


}

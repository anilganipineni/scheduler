package com.github.anilganipineni.scheduler.task.helper;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.CompletionHandler;
import com.github.anilganipineni.scheduler.task.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.ExecutionContext;
import com.github.anilganipineni.scheduler.task.ExecutionHandler;
import com.github.anilganipineni.scheduler.task.FailureHandler;
import com.github.anilganipineni.scheduler.task.schedule.Schedule;

/**
 * @author akganipineni
 */
public class TaskBuilder<T> {
    private static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofMinutes(5);
    private final String name;
    private Class<T> dataClass;
    private FailureHandler<T> onFailure;
    private DeadExecutionHandler<T> onDeadExecution;
    private ScheduleOnStartup<T> onStartup;

    public TaskBuilder(String name, Class<T> dataClass) {
        this.name = name;
        this.dataClass = dataClass;
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution<>();
        this.onFailure = new FailureHandler.OnFailureRetryLater<T>(DEFAULT_RETRY_INTERVAL);
    }

    public TaskBuilder<T> onFailureReschedule(Schedule schedule) {
        this.onFailure = new FailureHandler.OnFailureReschedule<T>(schedule);
        return this;
    }

    public TaskBuilder<T> onDeadExecutionRevive() {
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution<>();
        return this;
    }

    public TaskBuilder<T> onFailure(FailureHandler<T> failureHandler) {
        this.onFailure = failureHandler;
        return this;
    }

    public TaskBuilder<T> onDeadExecution(DeadExecutionHandler<T> deadExecutionHandler) {
        this.onDeadExecution = deadExecutionHandler;
        return this;
    }

    public TaskBuilder<T> scheduleOnStartup(String instance, T initialData, Function<Instant,Instant> firstExecutionTime) {
        this.onStartup = new ScheduleOnStartup<T>(instance, initialData, firstExecutionTime);
        return this;
    }

    public CustomTask<T> execute(ExecutionHandler<T> executionHandler) {
        return new CustomTask<T>(name, dataClass, onStartup, onFailure, onDeadExecution) {
            @Override
            public CompletionHandler<T> execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
                return executionHandler.execute(taskInstance, executionContext);
            }
        };
    }



}

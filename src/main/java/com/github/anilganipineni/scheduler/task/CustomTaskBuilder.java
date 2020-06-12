package com.github.anilganipineni.scheduler.task;

import java.time.Duration;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.ExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;
import com.github.anilganipineni.scheduler.task.handler.OnFailureReschedule;
import com.github.anilganipineni.scheduler.task.handler.OnFailureRetryLater;
import com.github.anilganipineni.scheduler.task.helper.ExecutionContext;
import com.github.anilganipineni.scheduler.task.schedule.Schedule;

/**
 * @author akganipineni
 */
public class CustomTaskBuilder {
    private static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofMinutes(5);
    private final String name;
    private FailureHandler onFailure;
    private DeadExecutionHandler onDeadExecution;
    /**
     * @param name
     */
    public CustomTaskBuilder(String name) {
        this.name = name;
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution();
        this.onFailure = new OnFailureRetryLater(DEFAULT_RETRY_INTERVAL);
    }
    /**
     * @param schedule
     * @return
     */
    public CustomTaskBuilder onFailureReschedule(Schedule schedule) {
        this.onFailure = new OnFailureReschedule(schedule);
        return this;
    }
    /**
     * @return
     */
    public CustomTaskBuilder onDeadExecutionRevive() {
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution();
        return this;
    }
    /**
     * @param failureHandler
     * @return
     */
    public CustomTaskBuilder onFailure(FailureHandler failureHandler) {
        this.onFailure = failureHandler;
        return this;
    }
    /**
     * @param deadExecutionHandler
     * @return
     */
    public CustomTaskBuilder onDeadExecution(DeadExecutionHandler deadExecutionHandler) {
        this.onDeadExecution = deadExecutionHandler;
        return this;
    }
    /**
     * @param executionHandler
     * @return
     */
    public CustomTask execute(ExecutionHandler executionHandler) {
        return new CustomTask(name, onFailure, onDeadExecution) {
            @Override
            public CompletionHandler execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
                return executionHandler.execute(taskInstance, executionContext);
            }
        };
    }



}

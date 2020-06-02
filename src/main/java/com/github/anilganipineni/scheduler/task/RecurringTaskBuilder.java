package com.github.anilganipineni.scheduler.task;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;
import com.github.anilganipineni.scheduler.task.handler.VoidExecutionHandler;
import com.github.anilganipineni.scheduler.task.helper.ExecutionContext;
import com.github.anilganipineni.scheduler.task.schedule.Schedule;

/**
 * @author akganipineni
 */
public class RecurringTaskBuilder {
    private final String name;
    private final Schedule schedule;
    private FailureHandler onFailure;
    private DeadExecutionHandler onDeadExecution;
    private Object initialData = null;

    public RecurringTaskBuilder(String name, Schedule schedule) {
        this.name = name;
        this.schedule = schedule;
        this.onFailure = new FailureHandler.OnFailureReschedule(schedule);
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution();
    }

    public RecurringTaskBuilder onFailureReschedule() {
        this.onFailure = new FailureHandler.OnFailureReschedule(schedule);
        return this;
    }

    public RecurringTaskBuilder onDeadExecutionRevive() {
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution();
        return this;
    }

    public RecurringTaskBuilder onFailure(FailureHandler failureHandler) {
        this.onFailure = failureHandler;
        return this;
    }

    public RecurringTaskBuilder onDeadExecution(DeadExecutionHandler deadExecutionHandler) {
        this.onDeadExecution = deadExecutionHandler;
        return this;
    }

    public RecurringTaskBuilder initialData(Object initialData) {
        this.initialData = initialData;
        return this;
    }

    public RecurringTask execute(VoidExecutionHandler executionHandler) {
        return new RecurringTask(name, onFailure, onDeadExecution, schedule, RecurringTask.INSTANCE, initialData) {

            @Override
            public void executeActual(ScheduledTasks taskInstance, ExecutionContext executionContext) {
                executionHandler.execute(taskInstance, executionContext);
            }
        };
    }


}

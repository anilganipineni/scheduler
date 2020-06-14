package com.github.anilganipineni.scheduler.task;

import com.github.anilganipineni.scheduler.ExecutionContext;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.schedule.Schedule;
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;
import com.github.anilganipineni.scheduler.task.handler.OnFailureReschedule;
import com.github.anilganipineni.scheduler.task.handler.ReviveDeadExecution;
import com.github.anilganipineni.scheduler.task.handler.VoidExecutionHandler;

/**
 * @author akganipineni
 */
public class RecurringTaskBuilder {
    private final String name;
    private FailureHandler onFailure;
    private DeadExecutionHandler onDeadExecution;
    
    private final Schedule schedule;
    private Object initialData = null;

    public RecurringTaskBuilder(String name, Schedule schedule) {
        this.name = name;
        this.schedule = schedule;
        this.onFailure = new OnFailureReschedule(schedule);
        this.onDeadExecution = new ReviveDeadExecution();
    }

    public RecurringTaskBuilder onFailureReschedule() {
        this.onFailure = new OnFailureReschedule(schedule);
        return this;
    }

    public RecurringTaskBuilder onDeadExecutionRevive() {
        this.onDeadExecution = new ReviveDeadExecution();
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

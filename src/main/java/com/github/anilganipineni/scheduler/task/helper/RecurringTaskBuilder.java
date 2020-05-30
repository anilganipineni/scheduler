package com.github.anilganipineni.scheduler.task.helper;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.ExecutionContext;
import com.github.anilganipineni.scheduler.task.FailureHandler;
import com.github.anilganipineni.scheduler.task.VoidExecutionHandler;
import com.github.anilganipineni.scheduler.task.schedule.Schedule;

/**
 * @author akganipineni
 */
public class RecurringTaskBuilder<T> {
    private final String name;
    private final Schedule schedule;
    private Class<T> dataClass;
    private FailureHandler<T> onFailure;
    private DeadExecutionHandler<T> onDeadExecution;
    private ScheduleOnStartup<T> scheduleOnStartup;

    public RecurringTaskBuilder(String name, Schedule schedule, Class<T> dataClass) {
        this.name = name;
        this.schedule = schedule;
        this.dataClass = dataClass;
        this.onFailure = new FailureHandler.OnFailureReschedule<>(schedule);
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution<>();
        this.scheduleOnStartup = new ScheduleOnStartup<>(RecurringTask.INSTANCE, null, schedule::getInitialExecutionTime);
    }

    public RecurringTaskBuilder<T> onFailureReschedule() {
        this.onFailure = new FailureHandler.OnFailureReschedule<>(schedule);
        return this;
    }

    public RecurringTaskBuilder<T> onDeadExecutionRevive() {
        this.onDeadExecution = new DeadExecutionHandler.ReviveDeadExecution<>();
        return this;
    }

    public RecurringTaskBuilder<T> onFailure(FailureHandler<T> failureHandler) {
        this.onFailure = failureHandler;
        return this;
    }

    public RecurringTaskBuilder<T> onDeadExecution(DeadExecutionHandler<T> deadExecutionHandler) {
        this.onDeadExecution = deadExecutionHandler;
        return this;
    }

    public RecurringTaskBuilder<T> initialData(T initialData) {
        this.scheduleOnStartup = new ScheduleOnStartup<>(RecurringTask.INSTANCE, initialData, schedule::getInitialExecutionTime);
        return this;
    }

    public RecurringTask<T> execute(VoidExecutionHandler<T> executionHandler) {
        return new RecurringTask<T>(name, schedule, dataClass, scheduleOnStartup, onFailure, onDeadExecution) {

            @Override
            public void executeRecurringly(ScheduledTasks taskInstance, ExecutionContext executionContext) {
                executionHandler.execute(taskInstance, executionContext);
            }
        };
    }


}

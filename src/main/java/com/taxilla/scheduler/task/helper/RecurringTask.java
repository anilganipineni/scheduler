/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.taxilla.scheduler.task.helper;

import com.taxilla.scheduler.Clock;
import com.taxilla.scheduler.Scheduler;
import com.taxilla.scheduler.task.*;
import com.taxilla.scheduler.task.CompletionHandler.OnCompleteReschedule;
import com.taxilla.scheduler.task.DeadExecutionHandler.ReviveDeadExecution;
import com.taxilla.scheduler.task.schedule.Schedule;

public abstract class RecurringTask<T> extends Task<T> implements OnStartup {

    public static final String INSTANCE = "recurring";
    private final OnCompleteReschedule<T> onComplete;
    private ScheduleOnStartup<T> scheduleOnStartup;

    public RecurringTask(String name, Schedule schedule, Class<T> dataClass) {
        this(name, schedule, dataClass, new ScheduleOnStartup<>(INSTANCE, null, schedule::getInitialExecutionTime), new FailureHandler.OnFailureReschedule<T>(schedule), new ReviveDeadExecution<>());
    }

    public RecurringTask(String name, Schedule schedule, Class<T> dataClass, T initialData) {
        this(name, schedule, dataClass, new ScheduleOnStartup<>(INSTANCE, initialData, schedule::getInitialExecutionTime), new FailureHandler.OnFailureReschedule<T>(schedule), new ReviveDeadExecution<>());
    }

    public RecurringTask(String name, Schedule schedule, Class<T> dataClass, ScheduleOnStartup<T> scheduleOnStartup, FailureHandler<T> failureHandler, DeadExecutionHandler<T> deadExecutionHandler) {
        super(name, dataClass, failureHandler, deadExecutionHandler);
        onComplete = new OnCompleteReschedule<>(schedule);
        this.scheduleOnStartup = scheduleOnStartup;
    }

    @Override
    public void onStartup(Scheduler scheduler, Clock clock) {
        if (scheduleOnStartup != null) {
                scheduleOnStartup.apply(scheduler, clock, this);
        }
    }

    @Override
    public CompletionHandler<T> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
        executeRecurringly(taskInstance, executionContext);
        return onComplete;
    }

    public abstract void executeRecurringly(TaskInstance<T> taskInstance, ExecutionContext executionContext);

}

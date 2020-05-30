/**
 * Copyright (C) Anil Ganipineni
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
package com.github.anilganipineni.scheduler.task.helper;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.CompletionHandler;
import com.github.anilganipineni.scheduler.task.CompletionHandler.OnCompleteReschedule;
import com.github.anilganipineni.scheduler.task.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.DeadExecutionHandler.ReviveDeadExecution;
import com.github.anilganipineni.scheduler.task.ExecutionContext;
import com.github.anilganipineni.scheduler.task.FailureHandler;
import com.github.anilganipineni.scheduler.task.OnStartup;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.schedule.Schedule;

public abstract class RecurringTask<T> extends Task<T> implements OnStartup {

    public static final String INSTANCE = "recurring";
    private final OnCompleteReschedule<T> onComplete;

    public RecurringTask(String name, Schedule schedule, Class<T> dataClass) {
        this(name, schedule, dataClass, new ScheduleOnStartup<>(INSTANCE, null, schedule::getInitialExecutionTime), new FailureHandler.OnFailureReschedule<T>(schedule), new ReviveDeadExecution<>());
    }

    public RecurringTask(String name, Schedule schedule, Class<T> dataClass, T initialData) {
        this(name, schedule, dataClass, new ScheduleOnStartup<>(INSTANCE, initialData, schedule::getInitialExecutionTime), new FailureHandler.OnFailureReschedule<T>(schedule), new ReviveDeadExecution<>());
    }

    public RecurringTask(String name, Schedule schedule, Class<T> dataClass, ScheduleOnStartup<T> scheduleOnStartup, FailureHandler<T> failureHandler, DeadExecutionHandler<T> deadExecutionHandler) {
        super(name, dataClass, scheduleOnStartup, failureHandler, deadExecutionHandler);
        onComplete = new OnCompleteReschedule<>(schedule);
    }

    @Override
    public CompletionHandler<T> execute(ScheduledTasks taskInstance, ExecutionContext executionContext) {
        executeRecurringly(taskInstance, executionContext);
        return onComplete;
    }

    public abstract void executeRecurringly(ScheduledTasks taskInstance, ExecutionContext executionContext);

}

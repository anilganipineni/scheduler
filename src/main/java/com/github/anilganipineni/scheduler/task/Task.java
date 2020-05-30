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
package com.github.anilganipineni.scheduler.task;

import com.github.anilganipineni.scheduler.Clock;
import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.helper.ScheduleOnStartup;

public abstract class Task<T> implements ExecutionHandler<T>, OnStartup {
    protected final String name;
    private final FailureHandler<T> failureHandler;
    private final DeadExecutionHandler<T> deadExecutionHandler;
    private final Class<T> dataClass;
    private ScheduleOnStartup<T> scheduleOnStartup;

    public Task(String name, Class<T> dataClass, ScheduleOnStartup<T> scheduleOnStartup, FailureHandler<T> failureHandler, DeadExecutionHandler<T> deadExecutionHandler) {
        this.name = name;
        this.dataClass = dataClass;
        this.failureHandler = failureHandler;
        this.deadExecutionHandler = deadExecutionHandler;
        this.scheduleOnStartup = scheduleOnStartup;
    }

    public String getName() {
        return name;
    }

    public Class<T> getDataClass() {
        return dataClass;
    }

    public ScheduledTasks instance(String id, T data) {
        return new ScheduledTasks(null, this.name, id, data);
    }

    public abstract CompletionHandler<T> execute(ScheduledTasks taskInstance, ExecutionContext executionContext);

    public FailureHandler<T> getFailureHandler() {
        return failureHandler;
    }

    public DeadExecutionHandler<T> getDeadExecutionHandler() {
        return deadExecutionHandler;
    }

    @Override
    public String toString() {
        return "Task " + "task=" + getName();
    }

    @Override
    public void onStartup(Scheduler scheduler, Clock clock) {
        if (scheduleOnStartup != null) {
                scheduleOnStartup.apply(scheduler, clock, this);
        }
    }

}

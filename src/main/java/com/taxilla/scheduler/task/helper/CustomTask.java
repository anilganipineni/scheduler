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
import com.taxilla.scheduler.task.DeadExecutionHandler;
import com.taxilla.scheduler.task.FailureHandler;
import com.taxilla.scheduler.task.OnStartup;
import com.taxilla.scheduler.task.Task;

public abstract class CustomTask<T> extends Task<T> implements OnStartup {
    private ScheduleOnStartup<T> scheduleOnStartup;

    public CustomTask(String name, Class<T> dataClass, ScheduleOnStartup<T> scheduleOnStartup, FailureHandler<T> failureHandler, DeadExecutionHandler<T> deadExecutionHandler) {
        super(name, dataClass, failureHandler, deadExecutionHandler);
        this.scheduleOnStartup = scheduleOnStartup;
    }

    @Override
    public void onStartup(Scheduler scheduler, Clock clock) {
        if (scheduleOnStartup != null) {
                scheduleOnStartup.apply(scheduler, clock, this);
        }
    }
}
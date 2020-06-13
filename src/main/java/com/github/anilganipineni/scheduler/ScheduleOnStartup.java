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
package com.github.anilganipineni.scheduler;

import java.time.Instant;
import java.util.function.Function;

import com.github.anilganipineni.scheduler.task.Task;

public class ScheduleOnStartup {
    String instance;
    Object data;
    Function<Instant, Instant> firstExecutionTime;

    public ScheduleOnStartup(String instance, Object data, Function<Instant, Instant> firstExecutionTime) {
        this.firstExecutionTime = firstExecutionTime;
        this.instance = instance;
        this.data = data;
    }

    public void apply(Scheduler scheduler, Clock clock, Task task) {
        if (data == null) {
            scheduler.schedule(task.instance(instance, null), firstExecutionTime.apply(clock.now()));
        } else {
            scheduler.schedule(task.instance(instance, data), firstExecutionTime.apply(clock.now()));
        }
    }

}

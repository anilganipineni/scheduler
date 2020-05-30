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

import com.github.anilganipineni.scheduler.task.schedule.Schedule;

/**
 * @author akganipineni
 */
public class Tasks {
    /**
     * @param name
     * @param schedule
     * @param dataClass
     * @return
     */
    public static <T> RecurringTaskBuilder<T> recurring(String name, Class<T> dataClass, Schedule schedule) {
        return new RecurringTaskBuilder<T>(name, schedule, dataClass);
    }
    /**
     * @param name
     * @param dataClass
     * @return
     */
    public static <T> OneTimeTaskBuilder<T> oneTime(String name, Class<T> dataClass) {
        return new OneTimeTaskBuilder<>(name, dataClass);
    }
    /**
     * @param name
     * @param dataClass
     * @return
     */
    public static <T> TaskBuilder<T> custom(String name, Class<T> dataClass) {
        return new TaskBuilder<>(name, dataClass);
    }
}

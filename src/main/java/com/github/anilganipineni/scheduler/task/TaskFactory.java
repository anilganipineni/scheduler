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

import com.github.anilganipineni.scheduler.schedule.Schedule;

/**
 * @author akganipineni
 */
public class TaskFactory {
    /**
     * @param name
     * @param schedule
     * @param dataClass
     * @return
     */
    public static RecurringTaskBuilder recurring(String name, Schedule schedule) {
        return new RecurringTaskBuilder(name, schedule);
    }
    /**
     * @param name
     * @param dataClass
     * @return
     */
    public static  OneTimeTaskBuilder oneTime(String name) {
        return new OneTimeTaskBuilder(name);
    }
    /**
     * @param name
     * @param dataClass
     * @return
     */
    public static  CustomTaskBuilder custom(String name) {
        return new CustomTaskBuilder(name);
    }
}

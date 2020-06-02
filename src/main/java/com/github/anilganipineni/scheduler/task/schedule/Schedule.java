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
package com.github.anilganipineni.scheduler.task.schedule;

import java.time.Instant;

import com.github.anilganipineni.scheduler.task.helper.ExecutionComplete;

/**
 * @author akganipineni
 */
public interface Schedule {
    /**
     * @param executionComplete
     * @return
     */
    Instant getNextExecutionTime(ExecutionComplete executionComplete);
    /**
     * Used to get the first execution-time for a schedule. Simulates an ExecutionComplete event.
     */
    default Instant getInitialExecutionTime(Instant now) {
        return getNextExecutionTime(ExecutionComplete.simulatedSuccess(now));
    }
}

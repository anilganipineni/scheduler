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
package com.github.anilganipineni.scheduler.task.handler;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.schedule.Schedule;
import com.github.anilganipineni.scheduler.task.helper.ExecutionComplete;
import com.github.anilganipineni.scheduler.task.helper.ExecutionOperations;

/**
 * @author akganipineni
 */
public class OnFailureReschedule implements FailureHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FailureHandler.class);
    /**
     * 
     */
    private final Schedule schedule;
    /**
     * @param schedule
     */
    public OnFailureReschedule(Schedule schedule) {
        this.schedule = schedule;
    }
    /**
	 * @see com.github.anilganipineni.scheduler.task.handler.FailureHandler#onFailure(com.github.anilganipineni.scheduler.task.helper.ExecutionComplete,
	 *      com.github.anilganipineni.scheduler.task.helper.ExecutionOperations)
	 */
    @Override
    public void onFailure(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
        Instant nextExecution = schedule.getNextExecutionTime(executionComplete);
        LOG.debug("ScheduledTasks failed. Rescheduling task {} to {}", executionComplete.getExecution(), nextExecution);
        executionOperations.reschedule(executionComplete, nextExecution);
    }
}

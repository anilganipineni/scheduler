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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.ExecutionOperations;
import com.github.anilganipineni.scheduler.schedule.Schedule;

/**
 * @author akganipineni
 */
public class OnCompleteReschedule implements CompletionHandler {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(OnCompleteReschedule.class);
    private final Schedule schedule;
	/**
	 * @param schedule
	 */
	public OnCompleteReschedule(Schedule schedule) {
		this.schedule = schedule;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.task.handler.CompletionHandler#complete(com.github.anilganipineni.scheduler.ExecutionComplete,
	 *      com.github.anilganipineni.scheduler.ExecutionOperations)
	 */
	@Override
	public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
        Instant nextExecution = schedule.getNextExecutionTime(executionComplete);
        logger.debug("Rescheduling task {} to {}", executionComplete.getExecution(), nextExecution);
        executionOperations.reschedule(executionComplete, nextExecution);	
	}
}

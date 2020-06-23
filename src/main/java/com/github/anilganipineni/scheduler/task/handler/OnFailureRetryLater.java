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

import java.time.Duration;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.ExecutionOperations;

/**
 * TODO: Failure handler with backoff: if (isFailing(.)) then nextTry = 2* duration_from_first_failure (minimum 1m, max 1d)
 * 
 * @author akganipineni
 */
public class OnFailureRetryLater implements FailureHandler {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(OnFailureRetryLater.class);
    private final Duration sleepDuration;

    public OnFailureRetryLater(Duration sleepDuration) {
        this.sleepDuration = sleepDuration;
    }

    @Override
    public void onFailure(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
        Instant nextTry = Instant.now().plus(sleepDuration);
        logger.debug("ScheduledTasks failed. Retrying task {} at {}", executionComplete.getExecution(), nextTry);
        executionOperations.reschedule(executionComplete, nextTry);
    }


}

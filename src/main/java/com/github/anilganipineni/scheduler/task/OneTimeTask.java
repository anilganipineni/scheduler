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

import java.time.Duration;

import com.github.anilganipineni.scheduler.ExecutionContext;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;
import com.github.anilganipineni.scheduler.task.handler.OnCompleteRemove;
import com.github.anilganipineni.scheduler.task.handler.OnFailureRetryLater;
import com.github.anilganipineni.scheduler.task.handler.ReviveDeadExecution;

public abstract class OneTimeTask extends Task {
	/**
	 * @param name
	 * @param failureHandler
	 * @param deadExecutionHandler
	 */
	public OneTimeTask(String name) {
        this(name, new OnFailureRetryLater(Duration.ofMinutes(5)), new ReviveDeadExecution());
	}
    /**
	 * @param name
	 * @param failureHandler
	 * @param deadExecutionHandler
	 * @param schedule
	 * @param instance
	 * @param data
	 */
	public OneTimeTask(String name, FailureHandler failureHandler, DeadExecutionHandler deadExecutionHandler) {
		super(name, failureHandler, deadExecutionHandler);
	}
	/**
	 * @see com.github.anilganipineni.scheduler.task.handler.ExecutionHandler#execute(com.github.anilganipineni.scheduler.dao.ScheduledTasks,
	 *      com.github.anilganipineni.scheduler.ExecutionContext)
	 */
	@Override
	public final CompletionHandler execute(ScheduledTasks task, ExecutionContext context) {
		executeActual(task, context);
		return  new OnCompleteRemove();
	}
	/**
	 * @param task
	 * @param context
	 */
	public abstract void executeActual(ScheduledTasks task, ExecutionContext context);
}

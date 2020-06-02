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

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;
import com.github.anilganipineni.scheduler.task.handler.OnCompleteReschedule;
import com.github.anilganipineni.scheduler.task.helper.ExecutionContext;
import com.github.anilganipineni.scheduler.task.schedule.Schedule;

public abstract class RecurringTask extends Task {
    public static final String INSTANCE = "recurring";
	/**
	 * @param name
	 * @param failureHandler
	 * @param deadExecutionHandler
	 */
	public RecurringTask(String name, FailureHandler failureHandler, DeadExecutionHandler deadExecutionHandler) {
		super(name, failureHandler, deadExecutionHandler);
	}
    /**
	 * @param name
	 * @param failureHandler
	 * @param deadExecutionHandler
	 * @param schedule
	 */
	public RecurringTask(String name, FailureHandler failureHandler, DeadExecutionHandler deadExecutionHandler, Schedule schedule) {
		this(name, failureHandler, deadExecutionHandler, schedule, INSTANCE, null);
	}
    /**
	 * @param name
	 * @param failureHandler
	 * @param deadExecutionHandler
	 * @param schedule
	 * @param instance
	 * @param data
	 */
	public RecurringTask(String name, FailureHandler failureHandler, DeadExecutionHandler deadExecutionHandler, Schedule schedule, String instance, Object data) {
		super(name, failureHandler, deadExecutionHandler, schedule, instance, data);
	}
	/**
	 * @see com.github.anilganipineni.scheduler.task.ExecutionHandler#execute(com.github.anilganipineni.scheduler.dao.ScheduledTasks,
	 *      com.github.anilganipineni.scheduler.task.helper.ExecutionContext)
	 */
	@Override
	public final CompletionHandler execute(ScheduledTasks task, ExecutionContext context) {
		executeActual(task, context);
		return  new OnCompleteReschedule(getSchedule());
	}
	/**
	 * @param task
	 * @param context
	 */
	public abstract void executeActual(ScheduledTasks task, ExecutionContext context);
}

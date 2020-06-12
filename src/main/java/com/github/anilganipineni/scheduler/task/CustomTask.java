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
import com.github.anilganipineni.scheduler.task.handler.DeadExecutionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;

public abstract class CustomTask extends Task {
	/**
	 * @param name
	 * @param failureHandler
	 * @param deadExecutionHandler
	 */
	public CustomTask(String name, FailureHandler failureHandler, DeadExecutionHandler deadExecutionHandler) {
		super(name, failureHandler, deadExecutionHandler);
	}
	/**
	 * @param name
	 * @param failureHandler
	 * @param deadExecutionHandler
	 * @param schedule
	 * @param instance
	 * @param data
	 */
	public CustomTask(String name, FailureHandler failureHandler, DeadExecutionHandler deadExecutionHandler, Schedule schedule, String instance, Object data) {
		super(name, failureHandler, deadExecutionHandler, schedule, instance, data);
	}
}

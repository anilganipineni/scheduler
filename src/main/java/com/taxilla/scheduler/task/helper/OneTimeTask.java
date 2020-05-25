/**
 * Copyright (C) Gustav Karlsson
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
package com.taxilla.scheduler.task.helper;

import java.time.Duration;

import com.taxilla.scheduler.task.*;
import com.taxilla.scheduler.task.CompletionHandler.OnCompleteRemove;
import com.taxilla.scheduler.task.DeadExecutionHandler.ReviveDeadExecution;
import com.taxilla.scheduler.task.FailureHandler.OnFailureRetryLater;

public abstract class OneTimeTask<T> extends Task<T> {

    public OneTimeTask(String name, Class<T> dataClass) {
        this(name, dataClass, new OnFailureRetryLater<>(Duration.ofMinutes(5)), new ReviveDeadExecution<>());
    }

    public OneTimeTask(String name, Class<T> dataClass, FailureHandler<T> failureHandler, DeadExecutionHandler<T> deadExecutionHandler) {
        super(name, dataClass, failureHandler, deadExecutionHandler);
    }

    @Override
    public CompletionHandler<T> execute(TaskInstance<T> taskInstance, ExecutionContext executionContext) {
        executeOnce(taskInstance, executionContext);
        return new OnCompleteRemove<>();
    }

    public abstract void executeOnce(TaskInstance<T> taskInstance, ExecutionContext executionContext);

}

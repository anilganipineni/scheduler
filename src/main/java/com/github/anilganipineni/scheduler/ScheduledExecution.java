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
package com.github.anilganipineni.scheduler;

import java.time.Instant;
import java.util.Objects;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.exception.SchedulerException;

/**
 * @author akganipineni
 */
public class ScheduledExecution<T> {
    /**
     * the data class
     */
    private final Class<T> dataClass;
    /**
     * 
     */
    private final ScheduledTasks execution;
    /**
     * @param dataClass
     * @param execution
     */
    public ScheduledExecution(Class<T> dataClass, ScheduledTasks execution) {
        this.dataClass = dataClass;
        this.execution = execution;
    }
    /**
     * @return
     */
    public Instant getExecutionTime() {
        return execution.getExecutionTime();
    }
    /**
     * @return
     * @throws SchedulerException
     */
    @SuppressWarnings("unchecked")
	public T getData() throws SchedulerException {
        if (dataClass.isInstance(this.execution.getTaskData())) {
            return (T) this.execution.getTaskData();
        }
        throw new SchedulerException();
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScheduledExecution<?> that = (ScheduledExecution<?>) o;
        return Objects.equals(execution, that.execution);
    }
    @Override
    public int hashCode() {
        return Objects.hash(execution);
    }
}

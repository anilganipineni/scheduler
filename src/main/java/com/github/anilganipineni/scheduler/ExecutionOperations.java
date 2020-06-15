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
import java.util.Map;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.dao.SchedulerRepository;

public class ExecutionOperations {

    private final SchedulerRepository<ScheduledTasks> taskRepository;
    private final ScheduledTasks execution;

    public ExecutionOperations(SchedulerRepository<ScheduledTasks> taskRepository, ScheduledTasks execution) {
        this.taskRepository = taskRepository;
        this.execution = execution;
    }

    public void stop() {
        taskRepository.remove(execution);
    }

    public void reschedule(ExecutionComplete completed, Instant nextExecutionTime) {
        if (completed.getResult() == ExecutionComplete.Result.OK) {
            taskRepository.reschedule(execution, nextExecutionTime, completed.getTimeDone(), execution.getLastFailure(), 0);
        } else {
            taskRepository.reschedule(execution, nextExecutionTime, execution.getLastSuccess(), completed.getTimeDone(), execution.getConsecutiveFailures() + 1);
        }

    }

    public void reschedule(ExecutionComplete completed, Instant nextExecutionTime, Map<String, Object> newData) {
        if (completed.getResult() == ExecutionComplete.Result.OK) {
            taskRepository.reschedule(execution, nextExecutionTime, completed.getTimeDone(), execution.getLastFailure(), 0, newData);
        } else {
            taskRepository.reschedule(execution, nextExecutionTime, execution.getLastSuccess(), completed.getTimeDone(), execution.getConsecutiveFailures() + 1, newData);
        }
    }

}

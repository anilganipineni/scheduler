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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.exception.SchedulerException;
import com.github.anilganipineni.scheduler.task.Task;

public interface Scheduler {

    void schedule(ScheduledTasks taskInstance, Instant executionTime);

    void reschedule(ScheduledTasks taskInstanceId, Instant newExecutionTime) throws SchedulerException;

    void reschedule(ScheduledTasks taskInstanceId, Instant newExecutionTime, Map<String, Object> newData) throws SchedulerException;

    void cancel(ScheduledTasks taskInstanceId) throws SchedulerException;

    void getScheduledExecutions(Consumer<ScheduledTasks> consumer) throws SchedulerException;

    void getScheduledExecutionsForTask(String taskName, Consumer<ScheduledTasks> consumer) throws SchedulerException;

    Optional<ScheduledTasks> getScheduledExecution(ScheduledTasks taskInstanceId) throws SchedulerException;
    
    public void start();
    public void stop();
    public void addTask(Task task);
    public void executeDue();
    public List<CurrentlyExecuting> getCurrentlyExecuting();
    public void detectDeadExecutions();
    public List<ScheduledTasks> getFailingExecutions(Duration failingAtLeastFor) throws SchedulerException;
}

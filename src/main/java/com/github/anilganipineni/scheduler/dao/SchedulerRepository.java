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
package com.github.anilganipineni.scheduler.dao;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.github.anilganipineni.scheduler.exception.SchedulerException;

/**
 * @author akganipineni
 */
public interface SchedulerRepository<T> {
    public static final String TABLE_NAME = "scheduled_tasks";

    boolean createIfNotExists(T execution);
    List<T> getDue(Instant now, int limit) throws SchedulerException;
    void getScheduledExecutions(Consumer<T> consumer) throws SchedulerException;
    void getScheduledExecutions(String taskName, Consumer<T> consumer) throws SchedulerException;

    void remove(T execution);
    boolean reschedule(T execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures);
    boolean reschedule(T execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures, Map<String, Object> newData);

    Optional<T> pick(T e, Instant timePicked) throws SchedulerException;

    List<T> getDeadExecutions(Instant olderThan) throws SchedulerException;

    void updateHeartbeat(T execution, Instant heartbeatTime) throws SchedulerException;

    List<T> getExecutionsFailingLongerThan(Duration interval) throws SchedulerException;

    Optional<T> getExecution(String taskName, String taskInstanceId) throws SchedulerException;

    int removeExecutions(String taskName) throws SchedulerException;
}

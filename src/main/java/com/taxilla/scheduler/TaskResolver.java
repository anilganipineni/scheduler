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
package com.taxilla.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taxilla.scheduler.stats.StatsRegistry;
import com.taxilla.scheduler.task.Task;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

@SuppressWarnings("rawtypes")
public class TaskResolver {
    private static final Logger LOG = LoggerFactory.getLogger(TaskResolver.class);
    private final StatsRegistry statsRegistry;
    private final Clock clock;
    private final Map<String, Task> taskMap;
    private final Map<String, UnresolvedTask> unresolvedTasks = new ConcurrentHashMap<>();

    public TaskResolver(StatsRegistry statsRegistry, Task<?>... knownTasks) {
        this(statsRegistry, Arrays.asList(knownTasks));
    }

    public TaskResolver(StatsRegistry statsRegistry, List<Task<?>> knownTasks) {
        this(statsRegistry, new SystemClock(), knownTasks);
    }

    public TaskResolver(StatsRegistry statsRegistry, Clock clock, List<Task<?>> knownTasks) {
        this.statsRegistry = statsRegistry;
        this.clock = clock;
        this.taskMap = knownTasks.stream().collect(Collectors.toMap(Task::getName, identity()));
    }

    public Optional<Task> resolve(String taskName) {
        Task task = taskMap.get(taskName);
        if (task == null) {
            addUnresolved(taskName);
            statsRegistry.register(StatsRegistry.SchedulerStatsEvent.UNRESOLVED_TASK);
            LOG.info("Found execution with unknown task-name '{}'. Adding it to the list of known unresolved task-names.", taskName);
        }
        return Optional.ofNullable(task);
    }

    private void addUnresolved(String taskName) {
        unresolvedTasks.putIfAbsent(taskName, new UnresolvedTask(taskName));
    }

    public void addTask(Task task) {
        taskMap.put(task.getName(), task);
    }

    public List<UnresolvedTask> getUnresolved() {
        return new ArrayList<>(unresolvedTasks.values());
    }

    public List<String> getUnresolvedTaskNames(Duration unresolvedFor) {
        return unresolvedTasks.values().stream()
            .filter(unresolved -> Duration.between(unresolved.firstUnresolved, clock.now()).toMillis() > unresolvedFor.toMillis())
            .map(UnresolvedTask::getTaskName)
            .collect(Collectors.toList());
    }

    public void clearUnresolved(String taskName) {
        unresolvedTasks.remove(taskName);
    }

    public class UnresolvedTask {
        private final String taskName;
        private final Instant firstUnresolved;

        public UnresolvedTask(String taskName) {
            this.taskName = taskName;
            firstUnresolved = clock.now();
        }

        public String getTaskName() {
            return taskName;
        }
    }
}

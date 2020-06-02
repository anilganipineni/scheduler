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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.dao.DataSourceType;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.dao.SchedulerRepository;
import com.github.anilganipineni.scheduler.dao.cassandra.CassandraTaskRepository;
import com.github.anilganipineni.scheduler.dao.rdbms.JdbcTaskRepository;
import com.github.anilganipineni.scheduler.stats.StatsRegistry;
import com.github.anilganipineni.scheduler.task.Task;

public interface SchedulerClient {

    <T> void schedule(ScheduledTasks taskInstance, Instant executionTime);

    void reschedule(ScheduledTasks taskInstanceId, Instant newExecutionTime);

    <T> void reschedule(ScheduledTasks taskInstanceId, Instant newExecutionTime, T newData);

    void cancel(ScheduledTasks taskInstanceId);

    void getScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer);

    <T> void getScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer);

    Optional<ScheduledExecution<Object>> getScheduledExecution(ScheduledTasks taskInstanceId);

    public class Builder {

        private final SchedulerDataSource dataSource;
        private List<Task> knownTasks;
        private final Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
        private String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;

        private Builder(SchedulerDataSource dataSource, List<Task> knownTasks) {
            this.dataSource = dataSource;
            this.knownTasks = knownTasks;
        }

        public static Builder create(SchedulerDataSource dataSource, Task ... knownTasks) {
            return new Builder(dataSource, Arrays.asList(knownTasks));
        }

        public static Builder create(SchedulerDataSource dataSource, List<Task> knownTasks) {
            return new Builder(dataSource, knownTasks);
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * @return
         */
        public SchedulerClient build() {
        	/*final SchedulerRepository r = DbUtils.getRepository(dataSource, tableName, new TaskResolver(StatsRegistry.NOOP, knownTasks), new SchedulerClientName(), serializer); FIXME*/

            final SchedulerRepository<ScheduledTasks> taskRepository;
            if(DataSourceType.RDBMS.equals(dataSource.dataSourceType())) {
            	taskRepository = new JdbcTaskRepository(dataSource.rdbmsDataSource(), tableName, new TaskResolver(StatsRegistry.NOOP, knownTasks), new SchedulerClientName(), serializer);
            } else {
            	taskRepository = new CassandraTaskRepository(dataSource.cassandraDataSource(), tableName, new TaskResolver(StatsRegistry.NOOP, knownTasks), new SchedulerClientName(), serializer);
            }
            return new StandardSchedulerClient(taskRepository);
        }
    }

    class StandardSchedulerClient implements SchedulerClient {

        private static final Logger LOG = LoggerFactory.getLogger(StandardSchedulerClient.class);
        protected final SchedulerRepository<ScheduledTasks> taskRepository;
        private SchedulerClientEventListener schedulerClientEventListener;

        StandardSchedulerClient(SchedulerRepository<ScheduledTasks> taskRepository) {
            this(taskRepository, SchedulerClientEventListener.NOOP);
        }

        StandardSchedulerClient(SchedulerRepository<ScheduledTasks> taskRepository, SchedulerClientEventListener schedulerClientEventListener) {
            this.taskRepository = taskRepository;
            this.schedulerClientEventListener = schedulerClientEventListener;
        }

        @Override
        public <T> void schedule(ScheduledTasks taskInstance, Instant executionTime) {
            boolean success = taskRepository.createIfNotExists(new ScheduledTasks(executionTime, taskInstance.getTaskName(), taskInstance.getId(), taskInstance.getTaskData()));
            if (success) {
                notifyListeners(ClientEvent.EventType.SCHEDULE, taskInstance, executionTime);
            }
        }

        @Override
        public void reschedule(ScheduledTasks taskInstanceId, Instant newExecutionTime) {
            reschedule(taskInstanceId, newExecutionTime, null);
        }

        @Override
        public <T> void reschedule(ScheduledTasks taskInstanceId, Instant newExecutionTime, T newData) {
            String taskName = taskInstanceId.getTaskName();
            String instanceId = taskInstanceId.getId();
            Optional<ScheduledTasks> execution = taskRepository.getExecution(taskName, instanceId);
            if(execution.isPresent()) {
                if(execution.get().isPicked()) {
                    throw new RuntimeException(String.format("Could not reschedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
                }

                boolean success;
                if (newData == null) {
                    success = taskRepository.reschedule(execution.get(), newExecutionTime, null, null, 0);
                } else {
                    success = taskRepository.reschedule(execution.get(), newExecutionTime, newData, null, null, 0);
                }

                if (success) {
                    notifyListeners(ClientEvent.EventType.RESCHEDULE, taskInstanceId, newExecutionTime);
                }
            } else {
                throw new RuntimeException(String.format("Could not reschedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
            }
        }

        @Override
        public void cancel(ScheduledTasks taskInstanceId) {
            String taskName = taskInstanceId.getTaskName();
            String instanceId = taskInstanceId.getId();
            Optional<ScheduledTasks> execution = taskRepository.getExecution(taskName, instanceId);
            if(execution.isPresent()) {
                if(execution.get().isPicked()) {
                    throw new RuntimeException(String.format("Could not cancel schedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
                }

                taskRepository.remove(execution.get());
                notifyListeners(ClientEvent.EventType.CANCEL, taskInstanceId, execution.get().executionTime);
            } else {
                throw new RuntimeException(String.format("Could not cancel schedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
            }
        }

        @Override
        public void getScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer) {
            taskRepository.getScheduledExecutions(execution -> consumer.accept(new ScheduledExecution<>(Object.class, execution)));
        }

        @Override
        public <T> void getScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer) {
            taskRepository.getScheduledExecutions(taskName, execution -> consumer.accept(new ScheduledExecution<>(dataClass, execution)));
        }

        @Override
        public Optional<ScheduledExecution<Object>> getScheduledExecution(ScheduledTasks taskInstanceId) {
            Optional<ScheduledTasks> e = taskRepository.getExecution(taskInstanceId.getTaskName(), taskInstanceId.getId());
            return e.map(oe -> new ScheduledExecution<>(Object.class, oe));
        }

        private void notifyListeners(ClientEvent.EventType eventType, ScheduledTasks taskInstanceId, Instant executionTime) {
            try {
                schedulerClientEventListener.newEvent(new ClientEvent(new ClientEvent.ClientEventContext(eventType, taskInstanceId.getTaskName(), taskInstanceId.getId(), taskInstanceId.getTaskData(), executionTime)));
            } catch (Exception e) {
                LOG.error("Error when notifying SchedulerClientEventListener.", e);
            }
        }
    }


    class SchedulerClientName implements SchedulerName {
        @Override
        public String getName() {
            return "SchedulerClient";
        }

    }

}

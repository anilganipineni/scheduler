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

import static com.github.anilganipineni.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;
import static com.github.anilganipineni.scheduler.Scheduler.THREAD_PREFIX;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.dao.DataSourceType;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.dao.SchedulerRepository;
import com.github.anilganipineni.scheduler.dao.cassandra.CassandraTaskRepository;
import com.github.anilganipineni.scheduler.dao.rdbms.JdbcTaskRepository;
import com.github.anilganipineni.scheduler.stats.StatsRegistry;
import com.github.anilganipineni.scheduler.task.OnStartup;
import com.github.anilganipineni.scheduler.task.Task;

public class SchedulerBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerBuilder.class);
    private static final int POLLING_CONCURRENCY_MULTIPLIER = 3;

    protected Clock clock = new SystemClock(); // if this is set, waiter-clocks must be updated

    // protected final DataSource dataSource;
    protected final SchedulerDataSource dataSource;
    protected SchedulerName schedulerName = new SchedulerName.Hostname();
    protected int executorThreads = 10;
    protected final List<Task<?>> knownTasks = new ArrayList<>();
    protected final List<Task<?>> startTasks = new ArrayList<>();
    protected Waiter waiter = new Waiter(Duration.ofSeconds(10), clock);
    protected int pollingLimit;
    protected boolean useDefaultPollingLimit;
    protected StatsRegistry statsRegistry = StatsRegistry.NOOP;
    protected Duration heartbeatInterval = Duration.ofMinutes(5);
    protected Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
    protected String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;
    protected boolean enableImmediateExecution = false;
    protected ExecutorService executorService;
    protected Duration deleteUnresolvedAfter = Duration.ofDays(14);
    /**
     * @param dataSource
     * @param knownTasks
     */
    public SchedulerBuilder(SchedulerDataSource dataSource, List<Task<?>> knownTasks) {
        this.dataSource = dataSource;
        this.knownTasks.addAll(knownTasks);
        this.pollingLimit = calculatePollingLimit();
        this.useDefaultPollingLimit = true;
    }
    /**
     * @param pollingInterval
     * @return
     */
    public SchedulerBuilder pollingInterval(Duration pollingInterval) {
        waiter = new Waiter(pollingInterval, clock);
        return this;
    }
    /**
     * @param pollingLimit
     * @return
     */
    public SchedulerBuilder pollingLimit(int pollingLimit) {
        if(pollingLimit <= 0) {
            throw new IllegalArgumentException("pollingLimit must be a positive integer");
        }
        this.pollingLimit = pollingLimit;
        this.useDefaultPollingLimit = false;
        return this;
    }
    /**
     * @param numberOfThreads
     * @return
     */
    public SchedulerBuilder threads(int numberOfThreads) {
        this.executorThreads = numberOfThreads;
        if(useDefaultPollingLimit) {
            this.pollingLimit = calculatePollingLimit();
        }
        return this;
    }
    /**
     * @param duration
     * @return
     */
    public SchedulerBuilder heartbeatInterval(Duration duration) {
        this.heartbeatInterval = duration;
        return this;
    }
    /**
     * @param executorService
     * @return
     */
    public SchedulerBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }
    /**
     * @param statsRegistry
     * @return
     */
    public SchedulerBuilder statsRegistry(StatsRegistry statsRegistry) {
        this.statsRegistry = statsRegistry;
        return this;
    }
    /**
     * @param schedulerName
     * @return
     */
    public SchedulerBuilder schedulerName(SchedulerName schedulerName) {
        this.schedulerName = schedulerName;
        return this;
    }
    /**
     * @param serializer
     * @return
     */
    public SchedulerBuilder serializer(Serializer serializer) {
        this.serializer = serializer;
        return this;
    }
    /**
     * @param tableName
     * @return
     */
    public SchedulerBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
    /**
     * @return
     */
    public SchedulerBuilder enableImmediateExecution() {
        this.enableImmediateExecution = true;
        return this;
    }
    /**
     * @param deleteAfter
     * @return
     */
    public SchedulerBuilder deleteUnresolvedAfter(Duration deleteAfter) {
        this.deleteUnresolvedAfter = deleteAfter;
        return this;
    }
    /**
     * @return
     */
    private int calculatePollingLimit() {
        return executorThreads * POLLING_CONCURRENCY_MULTIPLIER;
    }
    /**
     * @return
     */
    public Scheduler build() {
        if (pollingLimit < executorThreads) {
            LOG.warn("Polling-limit is less than number of threads. Should be equal or higher.");
        }
        final TaskResolver taskResolver = new TaskResolver(statsRegistry, clock, knownTasks);
        /*final SchedulerRepository taskRepository = DbUtils.getRepository(dataSource, tableName, taskResolver, schedulerName, serializer); FIXME*/

        final SchedulerRepository taskRepository;
        if(DataSourceType.RDBMS.equals(dataSource.dataSourceType())) {
        	taskRepository = new JdbcTaskRepository(dataSource.rdbmsDataSource(), tableName, taskResolver, schedulerName, serializer);
        } else {
        	taskRepository = new CassandraTaskRepository(dataSource.cassandraDataSource(), tableName, taskResolver, schedulerName, serializer);
        }

        ExecutorService candidateExecutorService = executorService;
        if (candidateExecutorService == null) {
            candidateExecutorService = Executors.newFixedThreadPool(executorThreads, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-"));
        }

        LOG.info("Creating scheduler with configuration: threads={}, pollInterval={}s, heartbeat={}s enable-immediate-execution={}, table-name={}, name={}",
            executorThreads,
            waiter.getWaitDuration().getSeconds(),
            heartbeatInterval.getSeconds(),
            enableImmediateExecution,
            tableName,
            schedulerName.getName());
        return new Scheduler(clock, taskRepository, taskResolver, executorThreads, candidateExecutorService,
                schedulerName, waiter, heartbeatInterval, enableImmediateExecution, statsRegistry, pollingLimit,
            deleteUnresolvedAfter, startTasks);
    }
    
    /**
     * @param startTasks
     * @return
     */
    public final <T> SchedulerBuilder startTasks(Task<T>... startTasks) {
        return startTasks(Arrays.asList(startTasks));
    }
    /**
     * @param startTasks
     * @return
     */
    public <T> SchedulerBuilder startTasks(List<Task<T>> startTasks) {
        knownTasks.addAll(startTasks);
        this.startTasks.addAll(startTasks);
        return this;
    }
}

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

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.Serializer;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.dao.rdbms.JdbcRunner;
import com.github.anilganipineni.scheduler.testhelper.DataSourceCassandra;

/**
 * @author akganipineni
 */
public abstract class BaseTaskRepository<T> {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(DataSourceCassandra.class);
    private final TaskResolver taskResolver;
    private final SchedulerName schedulerSchedulerName;
    private final JdbcRunner jdbcRunner;
    private final Serializer serializer;

    public BaseTaskRepository(DataSource dataSource, String tableName, TaskResolver taskResolver, SchedulerName schedulerSchedulerName) {
        this(dataSource, tableName, taskResolver, schedulerSchedulerName, Serializer.DEFAULT_JAVA_SERIALIZER);
    }

    public BaseTaskRepository(DataSource dataSource, String tableName, TaskResolver taskResolver, SchedulerName schedulerSchedulerName, Serializer serializer) {
        this.tableName = tableName;
        this.taskResolver = taskResolver;
        this.schedulerSchedulerName = schedulerSchedulerName;
        this.jdbcRunner = new JdbcRunner(dataSource);
        this.serializer = serializer;
    }
    private final String tableName;

    public Optional<ScheduledTasks> getExecution(ScheduledTasks taskInstance) {
        return getExecution(taskInstance.getTaskName(), taskInstance.getId());
    }

    public Optional<ScheduledTasks> getExecution(String taskName, String taskInstanceId) {
        final List<ScheduledTasks> executions = jdbcRunner.query("select * from " + tableName + " where task_name = ? and task_instance = ?",
                (PreparedStatement p) -> {
                    p.setString(1, taskName);
                    p.setString(2, taskInstanceId);
                },
                new ExecutionResultSetMapper(taskResolver, serializer)
        );
        
        if (executions.size() > 1) {
            throw new RuntimeException(String.format("Found more than one matching execution for task name/id combination: '%s'/'%s'", taskName, taskInstanceId));
        }

        return executions.size() == 1 ? Optional.ofNullable(executions.get(0)) : Optional.empty();
    }
}

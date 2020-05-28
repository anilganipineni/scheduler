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
package com.github.anilganipineni.scheduler.dao.cassandra;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.Serializer;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.dao.CassandraDataSource;
import com.github.anilganipineni.scheduler.dao.TaskRepository;
import com.github.anilganipineni.scheduler.task.Execution;
import com.github.anilganipineni.scheduler.testhelper.DataSourceCassandra;

/**
 * @author akganipineni
 */
public class CassandraTaskRepository implements TaskRepository {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(DataSourceCassandra.class);
    private final TaskResolver taskResolver;
    private final SchedulerName schedulerSchedulerName;
    private final Serializer serializer;
    private final String tableName;
    /**
     * @param dataSource
     * @param tableName
     * @param taskResolver
     * @param schedulerSchedulerName
     */
    public CassandraTaskRepository(CassandraDataSource dataSource, String tableName, TaskResolver taskResolver, SchedulerName schedulerSchedulerName) {
        this(dataSource, tableName, taskResolver, schedulerSchedulerName, Serializer.DEFAULT_JAVA_SERIALIZER);
    }
    /**
     * @param dataSource
     * @param tableName
     * @param taskResolver
     * @param schedulerSchedulerName
     * @param serializer
     */
    public CassandraTaskRepository(CassandraDataSource dataSource, String tableName, TaskResolver taskResolver, SchedulerName schedulerSchedulerName, Serializer serializer) {
        this.tableName = tableName;
        this.taskResolver = taskResolver;
        this.schedulerSchedulerName = schedulerSchedulerName;
        this.serializer = serializer;
    }
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#createIfNotExists(com.github.anilganipineni.scheduler.task.Execution)
	 */
	@Override
	public boolean createIfNotExists(Execution execution) {
		// TODO Auto-generated method stub
		return false;
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#getDue(java.time.Instant, int)
	 */
	@Override
	public List<Execution> getDue(Instant now, int limit) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#getScheduledExecutions(java.util.function.Consumer)
	 */
	@Override
	public void getScheduledExecutions(Consumer<Execution> consumer) {
		// TODO Auto-generated method stub
		
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#getScheduledExecutions(java.lang.String, java.util.function.Consumer)
	 */
	@Override
	public void getScheduledExecutions(String taskName, Consumer<Execution> consumer) {
		// TODO Auto-generated method stub
		
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#remove(com.github.anilganipineni.scheduler.task.Execution)
	 */
	@Override
	public void remove(Execution execution) {
		// TODO Auto-generated method stub
		
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#reschedule(com.github.anilganipineni.scheduler.task.Execution, java.time.Instant, java.time.Instant, java.time.Instant, int)
	 */
	@Override
	public boolean reschedule(Execution execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure,
			int consecutiveFailures) {
		// TODO Auto-generated method stub
		return false;
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#reschedule(com.github.anilganipineni.scheduler.task.Execution, java.time.Instant, java.lang.Object, java.time.Instant, java.time.Instant, int)
	 */
	@Override
	public boolean reschedule(Execution execution, Instant nextExecutionTime, Object newData, Instant lastSuccess,
			Instant lastFailure, int consecutiveFailures) {
		// TODO Auto-generated method stub
		return false;
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#pick(com.github.anilganipineni.scheduler.task.Execution, java.time.Instant)
	 */
	@Override
	public Optional<Execution> pick(Execution e, Instant timePicked) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#getDeadExecutions(java.time.Instant)
	 */
	@Override
	public List<Execution> getDeadExecutions(Instant olderThan) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#updateHeartbeat(com.github.anilganipineni.scheduler.task.Execution, java.time.Instant)
	 */
	@Override
	public void updateHeartbeat(Execution execution, Instant heartbeatTime) {
		// TODO Auto-generated method stub
		
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#getExecutionsFailingLongerThan(java.time.Duration)
	 */
	@Override
	public List<Execution> getExecutionsFailingLongerThan(Duration interval) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#getExecution(java.lang.String, java.lang.String)
	 */
	@Override
	public Optional<Execution> getExecution(String taskName, String taskInstanceId) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.dao.TaskRepository#removeExecutions(java.lang.String)
	 */
	@Override
	public int removeExecutions(String taskName) {
		// TODO Auto-generated method stub
		return 0;
	}




}

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

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.StringUtils;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.UnresolvedTask;
import com.github.anilganipineni.scheduler.exception.SchedulerException;

/**
 * @author akganipineni
 */
public class CassandraTaskRepository implements SchedulerRepository<ScheduledTasks> {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(CassandraTaskRepository.class);
	private static final String SELECT 				= " select * from " + TABLE_NAME;
	private static final String UPDATE				= " update " + TABLE_NAME;
	private static final String WHERE_PK_CK			= " where task_name = ?  and task_id = ? ";
	private static final String LIMIT				= " LIMIT 2000 ";
	/**
	 * The preparedStatementCache for GSP application
	 */
	private Map<String, PreparedStatement> preparedStatementCache = new HashMap<String, PreparedStatement>();
	private CassandraDataSource dataSource = null;
    private final TaskResolver taskResolver;
    private final SchedulerName schedulerName;
	/**
	 * The singleton MappingManager for the whole application
	 */
	private MappingManager m_mappingManager = null;
	/**
	 * @param dataSource
	 * @param taskResolver
	 * @param schedulerName
	 */
	public CassandraTaskRepository(CassandraDataSource dataSource, TaskResolver taskResolver, SchedulerName schedulerName) {
		this.dataSource = dataSource;
        this.taskResolver = taskResolver;
        this.schedulerName = schedulerName;
        
        m_mappingManager = new MappingManager(dataSource.getSession());
		if(m_mappingManager == null) {
			 throw new IllegalStateException("Failed to create Mapping Manager!");
		}
		logger.info("Created Cassandra Mapping Manager successfully.........................");
	}
	/**
	 * @param query
	 * @param params
	 */
	private ResultSet execute(String query, Object... params) {
		ResultSet rs = dataSource.getSession().execute(query, params);
		return rs;
	}
	/**
	 * @param entityType
	 * @return
	 */
	private <E> Mapper<E> getMapper(Class<E> entityType) {
		return m_mappingManager.mapper(entityType);
	}
	/**
	 * @param sqlQuery
	 * @param entityType
	 * @param params
	 * @return
	 * @throws SchedulerException
	 */
	private <E> Result<E> getResult(String sqlQuery, Class<E> entityType, Object... params) throws SchedulerException {
		BoundStatement bs = getBoundStatement(sqlQuery, params);
		return getResult(bs, entityType);
	}
	/**
	 * @param cql
	 * @param params
	 * @return
	 */
	private BoundStatement getBoundStatement(String cql, Object... params) {
        PreparedStatement ps = preparedStatementCache.get(cql);
        // no statement cached, create one and cache it now.
        if (ps == null) {
            ps = dataSource.getSession().prepare(cql);
           preparedStatementCache.put(cql, ps);
        }
        return ps.bind(params);
	}
	/**
	 * @param bs
	 * @param entityType
	 * @return
	 * @throws SchedulerException
	 */
	private <E> Result<E> getResult(BoundStatement bs, Class<E> entityType) throws SchedulerException {
		return getMapper(entityType).map(getResultSet(bs));
	}
	/**
	 * @param sqlQuery
	 * @param entityType
	 * @param params
	 * @return
	 * @throws SchedulerException
	 */
	private <E> E getSingleResult(String sqlQuery, Class<E> entityType, Object... params) throws SchedulerException {
		List<E> results = getResultList(sqlQuery, entityType, params);
		if(results.size() > 1) {
			logger.warn("Duplicate results found for the given Query!");
			throw new SchedulerException("Duplicate results found for the given Query!");
		}
		
		if(results.isEmpty()) {
			logger.trace("No Result Found for the given Query!");
			return null;
		}
		
		return results.get(0);
	}
	/**
	 * @param entity
	 * @param entityType
	 * @param columnName
	 * @param generateCurrentIndex
	 */
	private <E> void create(E entity, Class<E> entityType) {
		getMapper(entityType).save(entity);
	}
	/**
	 * @param entity
	 * @param entityType
	 * @return
	 */
	protected <E> E update(E entity, Class<E> entityType) {
		getMapper(entityType).save(entity);
		return entity;
	}
	/**
	 * @param entity
	 * @param entityType
	 */
	private <E> void delete(E entity, Class<E> entityType) {
		getMapper(entityType).delete(entity);
	}
	/**
	 * @param bs
	 * @return
	 * @throws SchedulerException
	 */
	private ResultSet getResultSet(BoundStatement bs) throws SchedulerException {
		try {
			return dataSource.getSession().execute(bs);
		} catch (DriverException ex) {
			// OperationTimedOutException is possible by datastax Driver
			throw new SchedulerException("Failed to fetch data from store", ex);
		}
	}
	/**
	 * @param cql
	 * @param entityType
	 * @param params
	 * @return
	 * @throws SchedulerException
	 */
	private <E> List<E> getResultList(String cql, Class<E> entityType, Object... params) throws SchedulerException {
		List<E> results = new ArrayList<E>();
	    Result<E> result = getResult(cql, entityType, params);
		for(E t : result) {
			results.add(t);
		}
		return results;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#createIfNotExists(java.lang.Object)
	 */
	@Override
	public boolean createIfNotExists(ScheduledTasks task) {
		try {
			String taskName		= task.getTaskName();
			String taskId		= task.getTaskId();
			List<ScheduledTasks> taskss = getResultList(SELECT + WHERE_PK_CK, ScheduledTasks.class, taskName, taskId);
			
			if(taskss == null || taskss.isEmpty()) {
				create(task, ScheduledTasks.class);
				return true;
			}
			
	        if (taskss.size() > 1) {
	            throw new SchedulerException(String.format("Found more than one matching execution for task name/id combination: '%s'/'%s'", taskName, taskId));
	        }
	        
	        // found only one
	        ScheduledTasks existingTask = taskss.get(0);
	        logger.debug("ScheduledTasks not created, it already exists. Due: {}", existingTask.getExecutionTime());
	        
		} catch (SchedulerException ex) {
            logger.error("ScheduledTasks not created, another thread created it.", ex);
		}
        
        return false;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#reschedule(java.lang.Object,
	 *      java.time.Instant, java.time.Instant, java.time.Instant, int)
	 */
	@Override
	public boolean reschedule(ScheduledTasks task, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
		return reschedule(task, nextExecutionTime, lastSuccess, lastFailure, consecutiveFailures, null);
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#reschedule(java.lang.Object,
	 *      java.time.Instant, java.time.Instant, java.time.Instant, int,
	 *      java.util.Map)
	 */
	@Override
	public boolean reschedule(ScheduledTasks task, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures, Map<String, Object> newData) {
		return rescheduleInternal(task, nextExecutionTime, lastSuccess, lastFailure, consecutiveFailures, newData);
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#getDue(java.time.Instant, int)
	 */
	@Override
	public List<ScheduledTasks> getDue(Instant now, int limit) throws SchedulerException {

		List<String> taskNames = taskResolver.getUnresolved().stream().map(UnresolvedTask::getTaskName).collect(Collectors.toList());
		
		// We are limiting fetch size to 2000, as we are not using any PK
		// Ideally there should not be more than 2000 tasks
		List<ScheduledTasks> all = getResultList(SELECT + LIMIT, ScheduledTasks.class);

		List<ScheduledTasks> dues = new ArrayList<ScheduledTasks>();
		for(ScheduledTasks t : all) {
			if(!t.isPicked() && !taskNames.contains(t.getTaskName()) && t.getExecutionTime().isBefore(now)) {
				dues.add(t);
			}
		}
		Collections.sort(dues, c);
		
		return dues;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#getScheduledExecutions(java.util.function.Consumer)
	 */
	@Override
	public void getScheduledExecutions(Consumer<ScheduledTasks> consumer) throws SchedulerException {

		List<String> taskNames = taskResolver.getUnresolved().stream().map(UnresolvedTask::getTaskName).collect(Collectors.toList());
		
		// We are limiting fetch size to 2000, as we are not using any PK
		// Ideally there should not be more than 2000 tasks
		List<ScheduledTasks> results = getResultList(SELECT + LIMIT, ScheduledTasks.class);

		List<ScheduledTasks> dues = new ArrayList<ScheduledTasks>();
		for(ScheduledTasks t : results) {
			if(!t.isPicked() && !taskNames.contains(t.getTaskName())) {
				dues.add(t);
			}
		}
		
		consume(dues, consumer);
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#getScheduledExecutions(java.lang.String,
	 *      java.util.function.Consumer)
	 */
	@Override
	public void getScheduledExecutions(String taskName, Consumer<ScheduledTasks> consumer) throws SchedulerException {
		
		List<ScheduledTasks> results = getResultList(SELECT + " where task_name = ? " + LIMIT, ScheduledTasks.class, taskName);
		List<ScheduledTasks> dues = new ArrayList<ScheduledTasks>();
		for(ScheduledTasks t : results) {
			if(!t.isPicked()) {
				dues.add(t);
			}
		}
		
		consume(dues, consumer);
	}
	/**
	 * @param tasks
	 * @param consumer
	 */
	private void consume(List<ScheduledTasks> tasks, Consumer<ScheduledTasks> consumer) {
		
		Collections.sort(tasks, c);

		for(ScheduledTasks t : tasks) {
			consumer.accept(t);
		}
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#remove(java.lang.Object)
	 */
	@Override
	public void remove(ScheduledTasks task) {
		try {
			int removed = 0;
			String cql = SELECT + WHERE_PK_CK;
			List<ScheduledTasks> tasks = getResultList(cql, ScheduledTasks.class, task.getTaskName(), task.getTaskId());
			if(tasks == null || tasks.isEmpty()) {
	            throw new RuntimeException("Expected one execution to be removed, but zero found. Indicates a bug.");
			}
			removed = tasks.size();
			for(ScheduledTasks t : tasks) {
				delete(t, ScheduledTasks.class);
			}

	        if (removed != 1) {
	            throw new RuntimeException("Expected one execution to be removed, but removed " + removed + ". Indicates a bug.");
	        }
		} catch (SchedulerException ex) {
            throw new RuntimeException(ex);
		}
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#removeExecutions(java.lang.String)
	 */
	@Override
	public int removeExecutions(String taskName) throws SchedulerException {
		int removed = 0;
		String cql = SELECT + " where task_name = ?";
		List<ScheduledTasks> tasks = getResultList(cql, ScheduledTasks.class, taskName);
		for(ScheduledTasks t : tasks) {
			delete(t, ScheduledTasks.class);
			removed++;
		}
		
		return removed;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#pick(java.lang.Object, java.time.Instant)
	 */
	@Override
	public Optional<ScheduledTasks> pick(ScheduledTasks e, Instant timePicked) {
		int updated = 0;
		String cql = UPDATE + " set picked = ?, picked_by = ?, last_heartbeat = ?, version = ? " + WHERE_PK_CK; /*" + and picked = ? "*/;
		
		ResultSet rs = execute(cql, true, StringUtils.truncate(schedulerName.getName(), 50), Timestamp.from(timePicked), e.getVersion() + 1, e.getTaskName(), e.getTaskId());
		updated = rs.getAvailableWithoutFetching();

        if (updated == 0) {
            logger.trace("Failed to pick execution. It must have been picked by another scheduler.", e);
            return Optional.empty();
            
        } else if (updated == 1) {
        	ScheduledTasks pickedTask = null;
			try {
				pickedTask = getSingleResult(SELECT + WHERE_PK_CK, ScheduledTasks.class, e.getTaskName(), e.getTaskId());
			} catch (SchedulerException ex) {
				pickedTask = null;
			}
            if (pickedTask == null) {
            	throw new IllegalStateException("Unable to find picked execution. Must have been deleted by another thread. Indicates a bug.");
            	
            } else if (!pickedTask.isPicked()) {
            	throw new IllegalStateException("Picked execution does not have expected state in database: " + pickedTask);
            }
            return Optional.ofNullable(pickedTask);
            
        } else {
            throw new IllegalStateException("Updated multiple rows when picking single execution. Should never happen since name and id is primary key. ScheduledTasks: " + e);
        }
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#getDeadExecutions(java.time.Instant)
	 */
	@Override
	public List<ScheduledTasks> getDeadExecutions(Instant olderThan) throws SchedulerException {


		List<String> taskNames = taskResolver.getUnresolved().stream().map(UnresolvedTask::getTaskName).collect(Collectors.toList());
		
		// We are limiting fetch size to 2000, as we are not using any PK
		// Ideally there should not be more than 2000 tasks
		List<ScheduledTasks> all = getResultList(SELECT + LIMIT, ScheduledTasks.class);

		List<ScheduledTasks> dues = new ArrayList<ScheduledTasks>();
		for(ScheduledTasks t : all) {
			if(t.isPicked() && !taskNames.contains(t.getTaskName()) && t.getLastHeartbeat().isBefore(olderThan)) {
				dues.add(t);
			}
		}
		Collections.sort(dues, c2);
		
		return dues;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#updateHeartbeat(java.lang.Object, java.time.Instant)
	 */
	@Override
	public void updateHeartbeat(ScheduledTasks e, Instant heartbeatTime) {
		String cql = UPDATE + " set last_heartbeat = ?" + WHERE_PK_CK;

		int updated = 0;
		ResultSet rs = execute(cql, Timestamp.from(heartbeatTime), e.getTaskName(), e.getTaskId());
		updated = rs.getAvailableWithoutFetching();

        if (updated == 0) {
            logger.trace("Did not update heartbeat. ScheduledTasks must have been removed or rescheduled.", e);
        } else {
            if (updated > 1) {
                throw new IllegalStateException("Updated multiple rows updating heartbeat for execution. Should never happen since name and id is primary key. ScheduledTasks: " + e);
            }
            logger.debug("Updated heartbeat for execution: " + e);
        }
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#getExecutionsFailingLongerThan(java.time.Duration)
	 */
	@Override
	public List<ScheduledTasks> getExecutionsFailingLongerThan(Duration interval) throws SchedulerException {

		List<String> taskNames = taskResolver.getUnresolved().stream().map(UnresolvedTask::getTaskName).collect(Collectors.toList());
		
		// We are limiting fetch size to 2000, as we are not using any PK
		// Ideally there should not be more than 2000 tasks
		List<ScheduledTasks> all = getResultList(SELECT + LIMIT, ScheduledTasks.class);

		List<ScheduledTasks> dues = new ArrayList<ScheduledTasks>();
		for(ScheduledTasks t : all) {
			if (!taskNames.contains(t.getTaskName())
					&& ((t.getLastFailure() != null && t.getLastSuccess() == null)
					||  (t.getLastFailure() != null && t.getLastSuccess().isBefore(Instant.now().minus(interval))))) {
				dues.add(t);
			}
		}
		
		return dues;
	
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#getExecution(java.lang.String, java.lang.String)
	 */
	@Override
	public Optional<ScheduledTasks> getExecution(String name, String instance) throws SchedulerException {
		List<ScheduledTasks> tasks = getResultList(SELECT + WHERE_PK_CK, ScheduledTasks.class, name, instance);
		
        if (tasks.size() > 1) {
            throw new SchedulerException(String.format("Found more than one matching execution for task name/id combination: '%s'/'%s'", name, instance));
        }

        return tasks.size() == 1 ? Optional.ofNullable(tasks.get(0)) : Optional.empty();
	}
    /**
     * @param task
     * @param nextExecutionTime
     * @param data
     * @param lastSuccess
     * @param lastFailure
     * @param consecutiveFailures
     * @return
     */
    private boolean rescheduleInternal(ScheduledTasks task, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures, Map<String, Object> data) {
    	int updated = 0;
    	String cql	= UPDATE + " set picked = ?, picked_by = ?, last_heartbeat = ?, last_success = ?, last_failure = ?, "
    				+ "consecutive_failures = ?, execution_time = ?, task_data = ?, version = ? " + WHERE_PK_CK;
    	
    	ResultSet rs = execute(cql, false,
				null,
				null, 
				Optional.ofNullable(lastSuccess).map(Timestamp::from).orElse(null),
				Optional.ofNullable(lastFailure).map(Timestamp::from).orElse(null), 
				consecutiveFailures,
				Timestamp.from(nextExecutionTime),
				StringUtils.convertMap2String(data),
				task.getVersion() + 1,
				task.getTaskName(),
				task.getTaskId());
		updated = rs.getAvailableWithoutFetching();


        if (updated != 1) {
            throw new RuntimeException("Expected one execution to be updated, but updated " + updated + ". Indicates a bug.");
        }
        return updated > 0;
    }
    /**
     * @author akganipineni
     */
    private static Comparator<ScheduledTasks> c = new Comparator<ScheduledTasks>() {
		/**
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(ScheduledTasks o1, ScheduledTasks o2) {
			if(o1 == null && o2 == null) {
				return 0;
			}
			if(o1 == null) {
				return 1;
			}
			if(o2 == null) {
				return -1;
			}
			
			return o1.getExecutionTime().compareTo(o2.getExecutionTime());
		}
    };
    /**
     * @author akganipineni
     */
    private static Comparator<ScheduledTasks> c2 = new Comparator<ScheduledTasks>() {
		/**
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(ScheduledTasks o1, ScheduledTasks o2) {
			if(o1 == null && o2 == null) {
				return 0;
			}
			if(o1 == null) {
				return 1;
			}
			if(o2 == null) {
				return -1;
			}
			
			return o1.getLastHeartbeat().compareTo(o2.getLastHeartbeat());
		}
    };
}

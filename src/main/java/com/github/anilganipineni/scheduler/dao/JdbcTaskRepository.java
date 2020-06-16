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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.StringUtils;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.UnresolvedTask;
import com.github.anilganipineni.scheduler.exception.SQLRuntimeException;

/**
 * @author akganipineni
 */
public class JdbcTaskRepository implements SchedulerRepository<ScheduledTasks> {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(JdbcTaskRepository.class);
    private final TaskResolver taskResolver;
    private final SchedulerName schedulerName;
    private final JdbcRunner jdbcRunner;
    /**
     * @param dataSource
     * @param taskResolver
     * @param schedulerName
     */
    public JdbcTaskRepository(DataSource dataSource, TaskResolver taskResolver, SchedulerName schedulerName) {
        this.taskResolver = taskResolver;
        this.schedulerName = schedulerName;
        this.jdbcRunner = new JdbcRunner(dataSource);
    }
    /**
     * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#createIfNotExists(java.lang.Object)
     */
    @Override
    public boolean createIfNotExists(ScheduledTasks execution) {
        try {
            Optional<ScheduledTasks> existingExecution = getExecution(execution);
            if (existingExecution.isPresent()) {
                logger.debug("ScheduledTasks not created, it already exists. Due: {}", existingExecution.get().getExecutionTime());
                return false;
            }

            jdbcRunner.execute(
                    "insert into " + TABLE_NAME + "(task_name, task_id, task_data, execution_time, picked, version) values(?, ?, ?, ?, ?, ?)",
                    (PreparedStatement p) -> {
                        p.setString(1, execution.getTaskName());
                        p.setString(2, execution.getTaskId());
                        p.setObject(3, execution.getTaskData());
                        p.setTimestamp(4, Timestamp.from(execution.getExecutionTime()));
                        p.setBoolean(5, false);
                        p.setLong(6, 1L);
                    });
            return true;

        } catch (SQLRuntimeException e) {
            logger.debug("Exception when inserting execution. Assuming it to be a constraint violation.", e);
            Optional<ScheduledTasks> existingExecution = getExecution(execution);
            if (!existingExecution.isPresent()) {
                throw new RuntimeException("Failed to add new execution.", e);
            }
            logger.debug("ScheduledTasks not created, another thread created it.");
            return false;
        }
    }
    /**
     * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#getScheduledExecutions(java.util.function.Consumer)
     */
    @Override
    public void getScheduledExecutions(Consumer<ScheduledTasks> consumer) {
        UnresolvedFilter unresolvedFilter = new UnresolvedFilter(taskResolver.getUnresolved());
        List<ScheduledTasks> tasks = jdbcRunner.execute(
                "select * from " + TABLE_NAME + " where picked = ? " + unresolvedFilter.andCondition() + " order by execution_time asc",
                (PreparedStatement p) -> {
                    int index = 1;
                    p.setBoolean(index++, false);
                    unresolvedFilter.setParameters(p, index);
                },
                new ScheduledTasksMapper(taskResolver)
        );
    	for(ScheduledTasks t : tasks) {
    		consumer.accept(t);
    	}
    }
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#getScheduledExecutions(java.lang.String,
	 *      java.util.function.Consumer)
	 */
    @Override
    public void getScheduledExecutions(String taskName, Consumer<ScheduledTasks> consumer) {
    	List<ScheduledTasks> tasks = jdbcRunner.execute(
                "select * from " + TABLE_NAME + " where picked = ? and task_name = ? order by execution_time asc",
                (PreparedStatement p) -> {
                    p.setBoolean(1, false);
                    p.setString(2, taskName);
                },
                new ScheduledTasksMapper(taskResolver)
        );
    	for(ScheduledTasks t : tasks) {
    		consumer.accept(t);
    	}
    }

    @Override
    public List<ScheduledTasks> getDue(Instant now, int limit) {
        final UnresolvedFilter unresolvedFilter = new UnresolvedFilter(taskResolver.getUnresolved());
        String sql = "select * from " + TABLE_NAME + " where picked = ? and execution_time <= ? " + unresolvedFilter.andCondition() + " order by execution_time asc";
        return jdbcRunner.execute(sql,
                (PreparedStatement p) -> {
                    int index = 1;
                    p.setBoolean(index++, false);
                    p.setTimestamp(index++, Timestamp.from(now));
                    unresolvedFilter.setParameters(p, index);
                    p.setMaxRows(limit);
                },
                new ScheduledTasksMapper(taskResolver)
        );
    }

    @Override
    public void remove(ScheduledTasks execution) {

        final int removed = jdbcRunner.execute("delete from " + TABLE_NAME + " where task_name = ? and task_id = ? and version = ?",
                ps -> {
                    ps.setString(1, execution.getTaskName());
                    ps.setString(2, execution.getTaskId());
                    ps.setLong(3, execution.getVersion());
                }
        );

        if (removed != 1) {
            throw new RuntimeException("Expected one execution to be removed, but removed " + removed + ". Indicates a bug.");
        }
    }
    /**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#reschedule(java.lang.Object,
	 *      java.time.Instant, java.time.Instant, java.time.Instant, int)
	 */
    @Override
    public boolean reschedule(ScheduledTasks execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        return reschedule(execution, nextExecutionTime, lastSuccess, lastFailure, consecutiveFailures, null);
    }
	/**
	 * @see com.github.anilganipineni.scheduler.dao.SchedulerRepository#reschedule(java.lang.Object,
	 *      java.time.Instant, java.time.Instant, java.time.Instant, int,
	 *      java.util.Map)
	 */
    @Override
    public boolean reschedule(ScheduledTasks execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures, Map<String, Object> newData) {
        return rescheduleInternal(execution, nextExecutionTime, newData, lastSuccess, lastFailure, consecutiveFailures);
    }
    /**
     * @param execution
     * @param nextExecutionTime
     * @param data
     * @param lastSuccess
     * @param lastFailure
     * @param consecutiveFailures
     * @return
     */
    private boolean rescheduleInternal(ScheduledTasks execution, Instant nextExecutionTime, Map<String, Object> data, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        final int updated = jdbcRunner.execute(
                "update " + TABLE_NAME + " set " +
                        "picked = ?, " +
                        "picked_by = ?, " +
                        "last_heartbeat = ?, " +
                        "last_success = ?, " +
                        "last_failure = ?, " +
                        "consecutive_failures = ?, " +
                        "execution_time = ?, " +
                        (data != null ? "task_data = ?, " : "") +
                        "version = version + 1 " +
                        "where task_name = ? " +
                        "and task_id = ? " +
                        "and version = ?",
                ps -> {
                    int index = 1;
                    ps.setBoolean(index++, false);
                    ps.setString(index++, null);
                    ps.setTimestamp(index++, null);
                    ps.setTimestamp(index++, Optional.ofNullable(lastSuccess).map(Timestamp::from).orElse(null));
                    ps.setTimestamp(index++, Optional.ofNullable(lastFailure).map(Timestamp::from).orElse(null));
                    ps.setInt(index++, consecutiveFailures);
                    ps.setTimestamp(index++, Timestamp.from(nextExecutionTime));
                    if (data != null) {
                        // may cause datbase-specific problems, might have to use setNull instead
                        ps.setObject(index++, StringUtils.convertMap2String(data));
                    }
                    ps.setString(index++, execution.getTaskName());
                    ps.setString(index++, execution.getTaskId());
                    ps.setLong(index++, execution.getVersion());
                });

        if (updated != 1) {
            throw new RuntimeException("Expected one execution to be updated, but updated " + updated + ". Indicates a bug.");
        }
        return updated > 0;
    }

    @Override
    public Optional<ScheduledTasks> pick(ScheduledTasks e, Instant timePicked) {
        final int updated = jdbcRunner.execute(
                "update " + TABLE_NAME + " set picked = ?, picked_by = ?, last_heartbeat = ?, version = version + 1 " +
                        "where picked = ? " +
                        "and task_name = ? " +
                        "and task_id = ? " +
                        "and version = ?",
                ps -> {
                    ps.setBoolean(1, true);
                    ps.setString(2, StringUtils.truncate(schedulerName.getName(), 50));
                    ps.setTimestamp(3, Timestamp.from(timePicked));
                    ps.setBoolean(4, false);
                    ps.setString(5, e.getTaskName());
                    ps.setString(6, e.getTaskId());
                    ps.setLong(7, e.getVersion());
                });

        if (updated == 0) {
            logger.trace("Failed to pick execution. It must have been picked by another scheduler.", e);
            return Optional.empty();
            
        } else if (updated == 1) {
            final Optional<ScheduledTasks> pickedExecution = getExecution(e);
            if (!pickedExecution.isPresent()) {
                throw new IllegalStateException("Unable to find picked execution. Must have been deleted by another thread. Indicates a bug.");
            } else if (!pickedExecution.get().isPicked()) {
                throw new IllegalStateException("Picked execution does not have expected state in database: " + pickedExecution.get());
            }
            return pickedExecution;
            
        } else {
            throw new IllegalStateException("Updated multiple rows when picking single execution. Should never happen since name and id is primary key. ScheduledTasks: " + e);
        }
    }

    @Override
    public List<ScheduledTasks> getDeadExecutions(Instant olderThan) {
        final UnresolvedFilter unresolvedFilter = new UnresolvedFilter(taskResolver.getUnresolved());
        return jdbcRunner.execute(
                "select * from " + TABLE_NAME + " where picked = ? and last_heartbeat <= ? " + unresolvedFilter.andCondition() + " order by last_heartbeat asc",
                (PreparedStatement p) -> {
                    int index = 1;
                    p.setBoolean(index++, true);
                    p.setTimestamp(index++, Timestamp.from(olderThan));
                    unresolvedFilter.setParameters(p, index);
                },
                new ScheduledTasksMapper(taskResolver)
        );
    }

    @Override
    public void updateHeartbeat(ScheduledTasks e, Instant newHeartbeat) {

        final int updated = jdbcRunner.execute(
                "update " + TABLE_NAME + " set last_heartbeat = ? " +
                        "where task_name = ? " +
                        "and task_id = ? " +
                        "and version = ?",
                ps -> {
                    ps.setTimestamp(1, Timestamp.from(newHeartbeat));
                    ps.setString(2, e.getTaskName());
                    ps.setString(3, e.getTaskId());
                    ps.setLong(4, e.getVersion());
                });

        if (updated == 0) {
            logger.trace("Did not update heartbeat. ScheduledTasks must have been removed or rescheduled.", e);
        } else {
            if (updated > 1) {
                throw new IllegalStateException("Updated multiple rows updating heartbeat for execution. Should never happen since name and id is primary key. ScheduledTasks: " + e);
            }
            logger.debug("Updated heartbeat for execution: " + e);
        }
    }

    @Override
    public List<ScheduledTasks> getExecutionsFailingLongerThan(Duration interval) {
        UnresolvedFilter unresolvedFilter = new UnresolvedFilter(taskResolver.getUnresolved());
        return jdbcRunner.execute(
                "select * from " + TABLE_NAME + " where " +
                        "    ((last_success is null and last_failure is not null)" +
                        "    or (last_failure is not null and last_success < ?)) " +
                        unresolvedFilter.andCondition(),
                (PreparedStatement p) -> {
                    int index = 1;
                    p.setTimestamp(index++, Timestamp.from(Instant.now().minus(interval)));
                    unresolvedFilter.setParameters(p, index);
                },
                new ScheduledTasksMapper(taskResolver)
        );
    }

    @Override
    public int removeExecutions(String taskName) {
        return jdbcRunner.execute("delete from " + TABLE_NAME + " where task_name = ?",
            (PreparedStatement p) -> {
                p.setString(1, taskName);
            });
    }

    public Optional<ScheduledTasks> getExecution(ScheduledTasks taskId) {
        return getExecution(taskId.getTaskName(), taskId.getTaskId());
    }

    public Optional<ScheduledTasks> getExecution(String taskName, String taskIdId) {
        final List<ScheduledTasks> executions = jdbcRunner.execute("select * from " + TABLE_NAME + " where task_name = ? and task_id = ?",
                (PreparedStatement p) -> {
                    p.setString(1, taskName);
                    p.setString(2, taskIdId);
                },
                new ScheduledTasksMapper(taskResolver)
        );
        
        if (executions.size() > 1) {
            throw new RuntimeException(String.format("Found more than one matching execution for task name/id combination: '%s'/'%s'", taskName, taskIdId));
        }

        return executions.size() == 1 ? Optional.ofNullable(executions.get(0)) : Optional.empty();
    }

    private static class UnresolvedFilter {
        private final List<UnresolvedTask> unresolved;

        public UnresolvedFilter(List<UnresolvedTask> unresolved) {
            this.unresolved = unresolved;
        }

        public String andCondition() {
            return unresolved.isEmpty() ? "" :
                "and task_name not in (" + unresolved.stream().map(ignored -> "?").collect(Collectors.joining(",")) + ")";
        }

        public int setParameters(PreparedStatement p, int index) throws SQLException {
            final List<String> unresolvedTasknames = unresolved.stream().map(UnresolvedTask::getTaskName).collect(Collectors.toList());
            for (String taskName : unresolvedTasknames) {
                p.setString(index++, taskName);
            }
            return index;
        }
    }
}

package com.github.anilganipineni.scheduler.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.task.Task;

/**
 * @author akganipineni
 */
public class ScheduledTasksMapper implements ResultSetMapper<List<ScheduledTasks>> {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(ScheduledTasksMapper.class);
    private final TaskResolver taskResolver;
	/**
	 * @param taskResolver
	 * @param serializer
	 */
	public ScheduledTasksMapper(TaskResolver taskResolver) {
		this.taskResolver = taskResolver;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.ResultSetMapper#map(java.sql.ResultSet)
	 */
	@Override
    public List<ScheduledTasks> map(ResultSet rs) throws SQLException {
		ScheduledTasks task = null;
		List<ScheduledTasks> tasks = new ArrayList<ScheduledTasks>();
        while (rs.next()) {
            String taskName = rs.getString("task_name");
            Optional<Task> oTask = taskResolver.resolve(taskName);

            if(!oTask.isPresent()) {
            	logger.warn("Failed to find implementation for task with name '{}'. "
            				+ "ScheduledTasks will be excluded from due. Either delete the execution from the database, or "
            				+ "add an implementation for it. The scheduler may be configured to automatically delete unresolved "
            				+ "tasks after a certain period of time.", taskName);
                continue;
            }

            String instanceId 			= rs.getString("task_id");
            Instant executionTime		= rs.getTimestamp("execution_time").toInstant();
            boolean picked				= rs.getBoolean("picked");
            String pickedBy				= rs.getString("picked_by");
            int consecutiveFailures		= rs.getInt("consecutive_failures"); // null-value is returned as 0 which is the preferred default
            Instant lastSuccess			= Optional.ofNullable(rs.getTimestamp("last_success")).map(Timestamp::toInstant).orElse(null);
            Instant lastFailure			= Optional.ofNullable(rs.getTimestamp("last_failure")).map(Timestamp::toInstant).orElse(null);
            Instant lastHeartbeat		= Optional.ofNullable(rs.getTimestamp("last_heartbeat")).map(Timestamp::toInstant).orElse(null);
            int version				= rs.getInt("version");
            
            String data					= rs.getString("task_data");
            // Supplier dataSupplier = memoize(() -> serializer.deserialize(Object.class, data));
            task = new ScheduledTasks(executionTime, taskName, instanceId, data, picked, pickedBy, lastSuccess, lastFailure, consecutiveFailures, lastHeartbeat, version);
            tasks.add(task);
        }

        return tasks;
    }
}

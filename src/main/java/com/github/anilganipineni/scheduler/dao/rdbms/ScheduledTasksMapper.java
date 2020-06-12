package com.github.anilganipineni.scheduler.dao.rdbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.Serializer;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.testhelper.CassandraDataSourceImpl;

/**
 * @author akganipineni
 */
public class ScheduledTasksMapper implements ResultSetMapper<List<ScheduledTasks>> {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(CassandraDataSourceImpl.class);
    private final TaskResolver taskResolver;
    private final Serializer serializer;
	/**
	 * @param taskResolver
	 * @param serializer
	 */
	public ScheduledTasksMapper(TaskResolver taskResolver, Serializer serializer) {
		this.taskResolver = taskResolver;
		this.serializer = serializer;
	}
	/**
	 * @see com.github.anilganipineni.scheduler.dao.rdbms.ResultSetMapper#map(java.sql.ResultSet)
	 */
	@SuppressWarnings("rawtypes") // FIXME --> @SuppressWarnings("rawtypes")
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

            String instanceId 			= rs.getString("task_instance");
            Instant executionTime		= rs.getTimestamp("execution_time").toInstant();
            boolean picked				= rs.getBoolean("picked");
            String pickedBy				= rs.getString("picked_by");
            int consecutiveFailures		= rs.getInt("consecutive_failures"); // null-value is returned as 0 which is the preferred default
            Instant lastSuccess			= Optional.ofNullable(rs.getTimestamp("last_success")).map(Timestamp::toInstant).orElse(null);
            Instant lastFailure			= Optional.ofNullable(rs.getTimestamp("last_failure")).map(Timestamp::toInstant).orElse(null);
            Instant lastHeartbeat		= Optional.ofNullable(rs.getTimestamp("last_heartbeat")).map(Timestamp::toInstant).orElse(null);
            long version				= rs.getLong("version");
            
            byte[] data					= rs.getBytes("task_data");
            Supplier dataSupplier = memoize(() -> serializer.deserialize(Object.class, data));
            task = new ScheduledTasks(executionTime, taskName, instanceId, dataSupplier, picked, pickedBy, lastSuccess, lastFailure, consecutiveFailures, lastHeartbeat, version);
            tasks.add(task);
        }

        return tasks;
    }

    private static <T> Supplier<T> memoize(Supplier<T> original) {
        return new Supplier<T>() {
            Supplier<T> delegate = this::firstTime;
            boolean initialized;
            public T get() {
                return delegate.get();
            }
            private synchronized T firstTime() {
                if(!initialized) {
                    T value = original.get();
                    delegate = () -> value;
                    initialized = true;
                }
                return delegate.get();
            }
        };
    }

}

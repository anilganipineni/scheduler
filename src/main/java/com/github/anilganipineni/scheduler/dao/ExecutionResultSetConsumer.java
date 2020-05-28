package com.github.anilganipineni.scheduler.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.Serializer;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.dao.rdbms.ResultSetMapper;
import com.github.anilganipineni.scheduler.task.Execution;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.TaskInstance;
import com.github.anilganipineni.scheduler.testhelper.DataSourceCassandra;

public class ExecutionResultSetConsumer implements ResultSetMapper<Void> {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(DataSourceCassandra.class);

    private final Consumer<Execution> consumer;
    private final TaskResolver taskResolver;
    private final Serializer serializer;
	/**
	 * @param consumer
	 * @param taskResolver
	 * @param serializer
	 */
	public ExecutionResultSetConsumer(Consumer<Execution> consumer, TaskResolver taskResolver, Serializer serializer) {
		this.consumer = consumer;
		this.taskResolver = taskResolver;
		this.serializer = serializer;
	}

	@Override
    public Void map(ResultSet rs) throws SQLException {

        while (rs.next()) {
            String taskName = rs.getString("task_name");
            Optional<Task> task = taskResolver.resolve(taskName);

            if (!task.isPresent()) {
            	logger.warn("Failed to find implementation for task with name '{}'. Execution will be excluded from due. Either delete the execution from the database, or add an implementation for it. The scheduler may be configured to automatically delete unresolved tasks after a certain period of time.", taskName);
                continue;
            }

            String instanceId = rs.getString("task_instance");
            byte[] data = rs.getBytes("task_data");

            Instant executionTime = rs.getTimestamp("execution_time").toInstant();

            boolean picked = rs.getBoolean("picked");
            final String pickedBy = rs.getString("picked_by");
            Instant lastSuccess = Optional.ofNullable(rs.getTimestamp("last_success"))
                    .map(Timestamp::toInstant).orElse(null);
            Instant lastFailure = Optional.ofNullable(rs.getTimestamp("last_failure"))
                    .map(Timestamp::toInstant).orElse(null);
            int consecutiveFailures = rs.getInt("consecutive_failures"); // null-value is returned as 0 which is the preferred default
            Instant lastHeartbeat = Optional.ofNullable(rs.getTimestamp("last_heartbeat"))
                    .map(Timestamp::toInstant).orElse(null);
            long version = rs.getLong("version");
            
            Class<Task> tclz = task.get().getDataClass();
            Supplier dataSupplier = memoize(() -> serializer.deserialize(tclz, data));
            this.consumer.accept(new Execution(executionTime, new TaskInstance(taskName, instanceId, dataSupplier), picked, pickedBy, lastSuccess, lastFailure, consecutiveFailures, lastHeartbeat, version));
        }

        return null;
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

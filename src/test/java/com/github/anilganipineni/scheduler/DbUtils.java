package com.github.anilganipineni.scheduler;

import com.github.anilganipineni.scheduler.dao.DataSourceType;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.dao.TaskRepository;
import com.github.anilganipineni.scheduler.dao.cassandra.CassandraTaskRepository;
import com.github.anilganipineni.scheduler.dao.rdbms.JdbcRunner;
import com.github.anilganipineni.scheduler.dao.rdbms.JdbcTaskRepository;
import com.github.anilganipineni.scheduler.dao.rdbms.Mappers;
import com.github.anilganipineni.scheduler.dao.rdbms.PreparedStatementSetter;
import com.google.common.io.CharStreams;

import javax.sql.DataSource;

import static com.github.anilganipineni.scheduler.dao.rdbms.JdbcTaskRepository.DEFAULT_TABLE_NAME;
import static com.github.anilganipineni.scheduler.dao.rdbms.PreparedStatementSetter.NOOP;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Consumer;

public class DbUtils {
	/**
	 * @param dataSource
	 * @param tableName
	 * @param taskResolver
	 * @param schedulerName
	 * @return
	 */
	public static final TaskRepository getRepository(SchedulerDataSource dataSource,
													 String tableName,
													 TaskResolver taskResolver,
													 SchedulerName schedulerName) {
        return getRepository(dataSource, tableName, taskResolver, schedulerName, Serializer.DEFAULT_JAVA_SERIALIZER);
	}
	/**
	 * @param dataSource
	 * @param tableName
	 * @param taskResolver
	 * @param schedulerName
	 * @param serializer
	 * @return
	 */
	public static final TaskRepository getRepository(SchedulerDataSource dataSource,
													 String tableName,
													 TaskResolver taskResolver,
													 SchedulerName schedulerName,
													 Serializer serializer) {
        final TaskRepository repository;
        if(DataSourceType.RDBMS.equals(dataSource.dataSourceType())) {
        	repository = new JdbcTaskRepository(dataSource.rdbmsDataSource(), tableName, taskResolver, schedulerName, serializer);
        } else {
        	// TODO FIXME
        	System.err.println("\n\n ****************************** FIXME ****************************** \n\n");
        	repository = new CassandraTaskRepository(dataSource.rdbmsDataSource(), tableName, taskResolver, schedulerName, serializer);
        }
        return repository;
	}

    public static void clearTables(DataSource dataSource) {
        new JdbcRunner(dataSource).execute("delete from " + DEFAULT_TABLE_NAME, NOOP);
    }

    public static Consumer<DataSource> runSqlResource(String resource) {
        return dataSource -> {

            final JdbcRunner jdbcRunner = new JdbcRunner(dataSource);
            try {
                final String statements = CharStreams.toString(new InputStreamReader(DbUtils.class.getResourceAsStream(resource)));
                jdbcRunner.execute(statements, NOOP);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static int countExecutions(DataSource dataSource) {
        return new JdbcRunner(dataSource).query("select count(*) from " + DEFAULT_TABLE_NAME,
            PreparedStatementSetter.NOOP, Mappers.SINGLE_INT);
    }
}

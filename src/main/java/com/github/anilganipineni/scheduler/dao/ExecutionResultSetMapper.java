package com.github.anilganipineni.scheduler.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.github.anilganipineni.scheduler.Serializer;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.dao.rdbms.ResultSetMapper;
import com.github.anilganipineni.scheduler.task.Execution;

/**
 * @author akganipineni
 */
public class ExecutionResultSetMapper implements ResultSetMapper<List<Execution>> {
    private final ArrayList<Execution> executions;
    private final ExecutionResultSetConsumer delegate;
    /**
     * @param taskResolver
     * @param serializer
     */
    public ExecutionResultSetMapper(TaskResolver taskResolver, Serializer serializer) {
        this.executions = new ArrayList<>();
        this.delegate = new ExecutionResultSetConsumer(executions::add, taskResolver, serializer);
    }
    /**
     * @see com.github.anilganipineni.scheduler.dao.rdbms.ResultSetMapper#map(java.sql.ResultSet)
     */
    @Override
    public List<Execution> map(ResultSet resultSet) throws SQLException {
        this.delegate.map(resultSet);
        return this.executions;
    }

}

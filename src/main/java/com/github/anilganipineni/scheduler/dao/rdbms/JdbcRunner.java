/**
 * Copyright (C) Anil Ganipineni
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.anilganipineni.scheduler.dao.rdbms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.exception.IntegrityConstraintViolation;
import com.github.anilganipineni.scheduler.exception.SQLRuntimeException;

/**
 * @author akganipineni
 */
public class JdbcRunner {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(JdbcRunner.class);
	/**
	 * The {@link DataSource} to access the RDBMS
	 */
	private final DataSource dataSource;
	/**
	 * @param dataSource
	 */
	public JdbcRunner(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	/**
	 * @param query
	 * @param setParameters
	 * @return
	 */
	public int execute(String query, PreparedStatementSetter setParameters) {
		return execute(query, setParameters, new AfterExecutionImpl1());
	}
	/**
	 * @param query
	 * @param setParameters
	 * @param mapper
	 * @return
	 */
	public <T> T execute(String query, PreparedStatementSetter setParameters, ResultSetMapper<T> mapper) {
		return execute(query, setParameters, new AfterExecutionImpl2<T>(mapper));
	}
	/**
	 * @param query
	 * @param setParameters
	 * @param afterExecution
	 * @return
	 */
	private <T> T execute(String query, PreparedStatementSetter setParameters, AfterExecution<T> afterExecution) {

		Connection conn = getConnection();
		PreparedStatement ps = null;
		try {

			try {
				ps = conn.prepareStatement(query);
			} catch (SQLException e) {
				throw new SQLRuntimeException("Error when preparing statement.", e);
			}

			try {
				logger.trace("Setting parameters of prepared statement.");
				setParameters.setParameters(ps);
			} catch (SQLException e) {
				throw new SQLRuntimeException(e);
			}
			try {
				logger.trace("Executing prepared statement");
				ps.execute();
				return afterExecution.doAfterExecution(ps);
			} catch (SQLException e) {
				throw translateException(e);
			}

		} finally {
			nonThrowingClose(ps);
			nonThrowingClose(conn);
		}
	}

	private SQLRuntimeException translateException(SQLException ex) {
		if (ex instanceof SQLIntegrityConstraintViolationException) {
			return new IntegrityConstraintViolation(ex);
		} else {
			return new SQLRuntimeException(ex);
		}
	}
	/**
	 * @return
	 */
	private Connection getConnection() {
		Connection c;
		logger.trace("Getting connection from datasource");
		try {
			c = dataSource.getConnection();
		} catch (SQLException ex) {
			throw new SQLRuntimeException("Unable to open connection", ex);
		}
		return c;
	}


	private void nonThrowingClose(AutoCloseable toClose) {
		if (toClose == null) {
			return;
		}
		try {
			logger.trace("Closing " + toClose.getClass().getSimpleName());
			toClose.close();
		} catch (Exception e) {
			logger.warn("Exception on close of " + toClose.getClass().getSimpleName(), e);
		}
	}
	/**
	 * @author akganipineni
	 */
	private class AfterExecutionImpl1 implements AfterExecution<Integer> {
		/**
		 * @see com.github.anilganipineni.scheduler.dao.rdbms.JdbcRunner.AfterExecution#doAfterExecution(java.sql.PreparedStatement)
		 */
		@Override
		public Integer doAfterExecution(PreparedStatement ps) throws SQLException {
			return ps.getUpdateCount();
		}
	}
	/**
	 * @author akganipineni
	 */
	private class AfterExecutionImpl2<T> implements AfterExecution<T> {
		private ResultSetMapper<T> mapper = null;
		/**
		 * @param mapper
		 */
		public AfterExecutionImpl2(ResultSetMapper<T> mapper) {
			this.mapper = mapper;
		}

		/**
		 * @see com.github.anilganipineni.scheduler.dao.rdbms.JdbcRunner.AfterExecution#doAfterExecution(java.sql.PreparedStatement)
		 */
		@Override
		public T doAfterExecution(PreparedStatement ps) throws SQLException {
			ResultSet rs = null;
			try {
				rs = ps.getResultSet();
				return mapper.map(rs);
			} catch (SQLException e) {
				throw new SQLRuntimeException(e);
			} finally {
				nonThrowingClose(rs);
			}
		}
	}
	interface AfterExecution<T> {
		T doAfterExecution(PreparedStatement executedPreparedStatement) throws SQLException;
	}

	interface DoWithResultSet<T> {
		T withResultSet(ResultSet rs) throws SQLException;
	}
}

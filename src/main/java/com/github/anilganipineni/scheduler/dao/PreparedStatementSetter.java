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
package com.github.anilganipineni.scheduler.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author akganipineni
 */
public interface PreparedStatementSetter {
	/**
	 * @param preparedStatement
	 * @throws SQLException
	 */
	public void setParameters(PreparedStatement preparedStatement) throws SQLException;
	/**
	 * 
	 */
	public static final PreparedStatementSetter NOOP = new PreparedStatementSetter() {
		/**
		 * @see com.github.anilganipineni.scheduler.dao.PreparedStatementSetter#setParameters(java.sql.PreparedStatement)
		 */
		@Override
		public void setParameters(PreparedStatement preparedStatement) throws SQLException {
		}
	};
}

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

import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.Serializer;
import com.github.anilganipineni.scheduler.TaskResolver;

/**
 * @author akganipineni
 */
public class DbUtils {
	/**
	 * @param ds
	 * @param resolver
	 * @param name
	 * @return
	 */
	public static final SchedulerRepository<ScheduledTasks> getRepository(SchedulerDataSource ds,
																		  TaskResolver resolver,
																		  SchedulerName name) {
        return getRepository(ds, resolver, name, Serializer.DEFAULT_JAVA_SERIALIZER);
	}
	/**
	 * @param ds
	 * @param resolver
	 * @param name
	 * @param serializer
	 * @return
	 */
	public static final SchedulerRepository<ScheduledTasks> getRepository(SchedulerDataSource ds,
																		  TaskResolver resolver,
																		  SchedulerName name,
																		  Serializer serializer) {
        SchedulerRepository<ScheduledTasks> repository;
        if(DataSourceType.RDBMS.equals(ds.dataSourceType())) {
        	repository = new JdbcTaskRepository(ds.rdbmsDataSource(), resolver, name, serializer);
        } else {
        	repository = new CassandraTaskRepository(ds.cassandraDataSource(), resolver, name);
        }
        return repository;
	}
}

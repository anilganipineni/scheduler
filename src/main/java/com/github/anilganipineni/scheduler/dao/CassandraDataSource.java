package com.github.anilganipineni.scheduler.dao;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;

/**
 * @author akganipineni
 */
public interface CassandraDataSource {
	public Session getSession();
	public MappingManager getMappingManager();
	public void closeSession();
}

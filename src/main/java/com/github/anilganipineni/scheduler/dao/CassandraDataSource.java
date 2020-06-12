package com.github.anilganipineni.scheduler.dao;

import com.datastax.driver.core.Session;

/**
 * @author akganipineni
 */
public interface CassandraDataSource {
	public Session getSession();
	public void closeSession();
}

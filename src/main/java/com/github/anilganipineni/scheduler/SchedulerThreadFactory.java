package com.github.anilganipineni.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author akganipineni
 */
public class SchedulerThreadFactory implements ThreadFactory {
    private final String prefix;
    private final ThreadFactory defaultThreadFactory;
	/**
	 * @param prefix
	 */
	public SchedulerThreadFactory(String prefix) {
		this.prefix = prefix;
        this.defaultThreadFactory = Executors.defaultThreadFactory();
	}
	/**
	 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
	 */
	@Override
	public Thread newThread(Runnable r) {
        final Thread thread = defaultThreadFactory.newThread(r);
        thread.setName(prefix + thread.getName());
        return thread;
	}
}

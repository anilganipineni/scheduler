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
package com.github.anilganipineni.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author akganipineni
 */
public class RunUntilShutdown implements Runnable {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(RunUntilShutdown.class);
    /**
     * The actual {@link Runnable} instance which supposed to run every time
     */
    private final Runnable actual;
    /**
     * The {@link Waiter} which decides the waits between each run of this {@link Runnable}
     */
    private final Waiter waiter;
    /**
     * The {@link SchedulerState}
     */
    private final SchedulerState state;
    /**
     * The {@link StatsRegistry}
     */
    private final StatsRegistry registry;
    /**
     * @param actual
     * @param waiter
     * @param state
     * @param registry
     */
    public RunUntilShutdown(Runnable actual, Waiter waiter, SchedulerState state, StatsRegistry registry) {
        this.actual = actual;
        this.waiter = waiter;
        this.state = state;
        this.registry = registry;
    }
    /**
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        while (!state.isShuttingDown()) {
            try {
                actual.run();
            } catch (Throwable e) {
                logger.error("Unhandled exception. Will keep running.", e);
                registry.register(StatsRegistry.SchedulerStatsEvent.UNEXPECTED_ERROR);
            }

            try {
                waiter.doWait();
            } catch (InterruptedException interruptedException) {
                if (state.isShuttingDown()) {
                    logger.debug("Thread '{}' interrupted due to shutdown.", Thread.currentThread().getName());
                } else {
                    logger.error("Unexpected interruption of thread. Will keep running.", interruptedException);
                    registry.register(StatsRegistry.SchedulerStatsEvent.UNEXPECTED_ERROR);
                }
            }
        }
    }
}

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

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author akganipineni
 */
public class ExecutorUtils {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(ExecutorUtils.class);
    /**
     * @param executor
     * @param timeout
     * @return
     */
    public static boolean shutdownNowAndAwaitTermination(ExecutorService executor, Duration timeout) {
        executor.shutdownNow();
        return awaitTermination(executor, timeout);
    }
    /**
     * @param executorService
     * @param timeout
     * @return
     */
    public static boolean shutdownAndAwaitTermination(ExecutorService executorService, Duration timeout) {
        executorService.shutdown();
        return awaitTermination(executorService, timeout);
    }
    /**
     * @param executor
     * @param timeout
     * @return
     */
    private static boolean awaitTermination(ExecutorService executor, Duration timeout) {
        try {
            return executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for termination of executor.", e);
            return false;
        }
    }
    /**
     * @param prefix
     * @return
     */
    public static ThreadFactory getThreadFactory(String prefix) {
        return new SchedulerThreadFactory(prefix);
    }
}

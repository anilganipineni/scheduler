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
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.dao.CassandraTaskRepository;
import com.github.anilganipineni.scheduler.schedule.Clock;
import com.github.anilganipineni.scheduler.schedule.SystemClock;

/**
 * @author akganipineni
 */
public class Waiter {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(CassandraTaskRepository.class);
    private Object lock;
    private boolean woken = false;
    private final Duration duration;
    private Clock clock;
    private boolean isWaiting = false;
    /**
     * @param duration
     */
    public Waiter(Duration duration) {
        this(duration, new SystemClock());
    }
    /**
     * @param duration
     * @param clock
     */
    public Waiter(Duration duration, Clock clock) {
        this(duration, clock, new Object());
    }
    /**
     * @param duration
     * @param clock
     * @param lock
     */
    public Waiter(Duration duration, Clock clock, Object lock) {
        this.duration = duration;
        this.clock = clock;
        this.lock = lock;
    }
    /**
     * @throws InterruptedException
     */
    public void doWait() throws InterruptedException {
        final long millis = duration.toMillis();
        if (millis > 0) {
            Instant waitUntil = clock.now().plusMillis(millis);
            while(clock.now().isBefore(waitUntil)) {
                synchronized (lock) {
                    woken = false;
                    logger.debug("Waiter start wait.");
                    this.isWaiting = true;
                    lock.wait(millis);
                    this.isWaiting = false;
                    if (woken) {
                        logger.debug("Waiter woken, it had {}ms left to wait.", (waitUntil.toEpochMilli() - clock.now().toEpochMilli()));
                        break;
                    }
                }
            }
        }
    }
    /**
     * @return
     */
    public boolean wake() {
        synchronized (lock) {
            if (!isWaiting) {
                return false;
            } else {
                woken = true;
                lock.notify();
                return true;
            }
        }
    }
    /**
     * @return
     */
    public Duration getWaitDuration() {
        return duration;
    }

}

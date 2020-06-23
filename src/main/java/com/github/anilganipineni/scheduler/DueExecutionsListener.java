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

import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.schedule.Clock;

/**
 * @author akganipineni
 */
class DueExecutionsListener implements SchedulerEventListener {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(DueExecutionsListener.class);
    private SchedulerState schedulerState;
    private Clock clock;
    private Waiter executeDueWaiter;
    /**
     * @param state
     * @param clock
     * @param waiter
     */
    public DueExecutionsListener(SchedulerState state, Clock clock, Waiter waiter) {
        this.schedulerState = state;
        this.clock = clock;
        this.executeDueWaiter = waiter;
    }
	/**
	 * @see com.github.anilganipineni.scheduler.SchedulerEventListener#newEvent(com.github.anilganipineni.scheduler.EventContext)
	 */
	@Override
	public void newEvent(EventContext ctx) {
        EventType eventType = ctx.getEventType();

        if (!schedulerState.isStarted() || schedulerState.isShuttingDown()) {
            logger.debug("Will not act on scheduling event for execution (task: '{}', id: '{}') as scheduler is starting or shutting down.",
                    ctx.getTaskName(), ctx.getId());
            return;
        }

        if (eventType == EventType.SCHEDULE || eventType == EventType.RESCHEDULE) {

            Instant scheduledToExecutionTime = ctx.getExecutionTime();
            if (scheduledToExecutionTime.toEpochMilli() <= clock.now().toEpochMilli()) {
                logger.info("Task-instance scheduled to run directly, triggering check for due exections (unless it is already running). Task: {}, instance: {}",
                        ctx.getTaskName(), ctx.getId());
                executeDueWaiter.wake();
            }
        }
	}
}

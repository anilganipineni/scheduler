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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DueExecutionsBatch {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(DueExecutionsBatch.class);
    private final int generationNumber;
    private final AtomicInteger executionsLeftInBatch;
    private int threadpoolSize;
    private boolean possiblyMoreExecutionsInDb;
    private boolean stale = false;
    private boolean triggeredExecuteDue;

    public DueExecutionsBatch(int threadpoolSize, int generationNumber, int executionsAdded, boolean possiblyMoreExecutionsInDb) {
        this.threadpoolSize = threadpoolSize;
        this.generationNumber = generationNumber;
        this.possiblyMoreExecutionsInDb = possiblyMoreExecutionsInDb;
        this.executionsLeftInBatch = new AtomicInteger(executionsAdded);
    }

    public void markBatchAsStale() {
        this.stale = true;
    }

    /**
     *
     * @param triggerCheckForNewBatch may be triggered more than one in racy conditions
     */
    public void oneExecutionDone(Supplier<Boolean> triggerCheckForNewBatch) {
        executionsLeftInBatch.decrementAndGet();

        logger.trace("Batch state: stale:{}, triggeredExecuteDue:{}, possiblyMoreExecutionsInDb:{}, executionsLeftInBatch:{}, ratio-trigger:{}",
                stale, triggeredExecuteDue, possiblyMoreExecutionsInDb, executionsLeftInBatch.get(), (threadpoolSize * SchedulerImpl.TRIGGER_NEXT_BATCH_WHEN_AVAILABLE_THREADS_RATIO));
        if (!stale
                && !triggeredExecuteDue
                && possiblyMoreExecutionsInDb
                && executionsLeftInBatch.get() <= (threadpoolSize * SchedulerImpl.TRIGGER_NEXT_BATCH_WHEN_AVAILABLE_THREADS_RATIO)) {
            triggeredExecuteDue = triggerCheckForNewBatch.get();
        }
    }

    public boolean isOlderGenerationThan(int compareTo) {
        return generationNumber < compareTo;
    }

    public int getGenerationNumber() {
        return generationNumber;
    }
}

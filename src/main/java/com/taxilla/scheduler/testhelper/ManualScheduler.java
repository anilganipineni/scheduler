/**
 * Copyright (C) Gustav Karlsson
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
package com.taxilla.scheduler.testhelper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taxilla.scheduler.Scheduler;
import com.taxilla.scheduler.SchedulerName;
import com.taxilla.scheduler.TaskRepository;
import com.taxilla.scheduler.TaskResolver;
import com.taxilla.scheduler.Waiter;
import com.taxilla.scheduler.stats.StatsRegistry;
import com.taxilla.scheduler.task.OnStartup;

public class ManualScheduler extends Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(ManualScheduler.class);
    private final SettableClock clock;

    ManualScheduler(SettableClock clock, TaskRepository taskRepository, TaskResolver taskResolver, int maxThreads, ExecutorService executorService, SchedulerName schedulerName, Waiter waiter, Duration heartbeatInterval, boolean executeImmediately, StatsRegistry statsRegistry, int pollingLimit, Duration deleteUnresolvedAfter, List<OnStartup> onStartup) {
        super(clock, taskRepository, taskResolver, maxThreads, executorService, schedulerName, waiter, heartbeatInterval, executeImmediately, statsRegistry, pollingLimit, deleteUnresolvedAfter, onStartup);
        this.clock = clock;
    }

    public SettableClock getClock() {
        return clock;
    }

    public void tick(Duration moveClockForward) {
        clock.set(clock.now.plus(moveClockForward));
    }

    public void setTime(Instant newtime) {
        clock.set(newtime);
    }

    public void runAnyDueExecutions() {
        super.executeDue();
    }

    public void runDeadExecutionDetection() {
        super.detectDeadExecutions();
    }


    public void start() {
        LOG.info("Starting manual scheduler. Executing on-startup tasks.");
        executeOnStartup();
    }

}
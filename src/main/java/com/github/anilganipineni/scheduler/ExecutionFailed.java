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
import java.util.Optional;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;

public class ExecutionFailed {
    private final ScheduledTasks execution;
    private final Instant timeDone;
    private final Throwable cause;

    public ExecutionFailed(ScheduledTasks execution, Instant timeDone, Throwable cause) {
        this.cause = cause;
        this.execution = execution;
        this.timeDone = timeDone;
    }

    public ScheduledTasks getExecution() {
        return execution;
    }

    public Instant getTimeDone() {
        return timeDone;
    }

    public Optional<Throwable> getCause() {
        return Optional.ofNullable(cause);
    }

}

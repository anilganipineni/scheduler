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

import com.github.anilganipineni.scheduler.schedule.Clock;

/**
 * @author akganipineni
 */
public class CurrentlyExecuting {

    private final Clock clock;
    private final Instant startTime;

    public CurrentlyExecuting(Clock clock) {
        this.clock = clock;
        this.startTime = clock.now();
    }

    public Duration getDuration() {
        return Duration.between(startTime, clock.now());
    }
}

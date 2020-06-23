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
package com.github.anilganipineni.scheduler.schedule;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.github.anilganipineni.scheduler.ExecutionComplete;

/**
 * Spring-style cron-pattern schedule
 * 
 * @author akganipineni
 */
public class CronSchedule implements Schedule {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(CronSchedule.class);
    private final ExecutionTime cronExecutionTime;
    private final ZoneId zoneId;

    public CronSchedule(String pattern, ZoneId zoneId) {
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING));
        Cron cron = parser.parse(pattern);
        this.cronExecutionTime = ExecutionTime.forCron(cron);

        if (zoneId == null) {
            throw new IllegalArgumentException("zoneId may not be null");
        }
        this.zoneId = zoneId;
    }

    public CronSchedule(String pattern) {
        this(pattern, ZoneId.systemDefault());
    }

    @Override
    public Instant getNextExecutionTime(ExecutionComplete executionComplete) {
        ZonedDateTime lastDone = ZonedDateTime.ofInstant(executionComplete.getTimeDone(), zoneId);  //frame the 'last done' time in the context of the time zone for this schedule
        //so that expressions like "0 05 13,20 * * ?" (New York) can operate in the
        // context of the desired time zone
        Optional<ZonedDateTime> nextTime = cronExecutionTime.nextExecution(lastDone);
        if (!nextTime.isPresent()) {
            logger.error("Cron-pattern did not return any further execution-times. This behavior is currently not supported by the scheduler. Setting next execution-time to far-future.");
            return Instant.now().plus(1000, ChronoUnit.YEARS);
        }
        return nextTime.get().toInstant();
    }
}

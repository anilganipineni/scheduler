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

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;

import com.github.anilganipineni.scheduler.exception.UnrecognizableSchedule;
import com.github.anilganipineni.scheduler.parser.CompositeParser;
import com.github.anilganipineni.scheduler.parser.DailyParser;
import com.github.anilganipineni.scheduler.parser.FixedDelayParser;
import com.github.anilganipineni.scheduler.parser.Parser;

/**
 * @author akganipineni
 */
public class ScheduleFactory {
    private static final Parser SCHEDULE_PARSER = CompositeParser.of(new FixedDelayParser(), new DailyParser());
    /**
     * @param times
     * @return
     */
    public static Schedule daily(LocalTime... times) {
        return new Daily(times);
    }
    /**
     * @param zone
     * @param times
     * @return
     */
    public static Schedule daily(ZoneId zone, LocalTime... times) {
        return new Daily(zone, times);
    }
    /**
     * @param delay
     * @return
     */
    public static Schedule fixedDelay(Duration delay) {
        return FixedDelay.of(delay);
    }
    /**
     * @param cronPattern
     * @return
     */
    public static Schedule cron(String cronPattern) {
        return new CronSchedule(cronPattern);
    }
    /**
     * @param cronPattern
     * @param zoneId
     * @return
     */
    public static Schedule cron(String cronPattern, ZoneId zoneId) {
        return new CronSchedule(cronPattern, zoneId);
    }
    /**
     * Currently supports Daily- and FixedDelay-schedule on the formats:
     * <pre>DAILY|hh:mm,hh:mm,...,hh:mm(|TIME_ZONE)</pre><br/>
     * <pre>FIXED_DELAY|xxxs  (xxx is number of seconds)</pre>
     *
     * @param regEx
     * @return A new schedule
     * @throws UnrecognizableSchedule When the scheduleString cannot be parsed
     */
    public static Schedule parseSchedule(String regEx) {
        return SCHEDULE_PARSER.parse(regEx).orElseThrow(() -> new UnrecognizableSchedule(regEx, SCHEDULE_PARSER.examples()));
    }
}

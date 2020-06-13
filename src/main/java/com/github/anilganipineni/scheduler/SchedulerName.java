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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author akganipineni
 */
public interface SchedulerName {
    String getName();
    /**
     * @author akganipineni
     */
    class Fixed implements SchedulerName {
        private final String name;
        /**
         * default constructor with fixed name
         */
        public Fixed() {
            this("FixedNameSchedulerClient");
        }
        /**
         * @param name
         */
        public Fixed(String name) {
            this.name = name;
        }
        /**
         * @see com.github.anilganipineni.scheduler.SchedulerName#getName()
         */
        @Override
        public String getName() {
            return name;
        }
    }
    /**
     * @author akganipineni
     */
    class Hostname implements SchedulerName {
        /**
         * The <code>Logger</code> instance for this class.
         */
    	private static final Logger logger = LogManager.getLogger(Hostname.class);
        private String cachedHostname;
        /**
         * Default constructor
         */
        public Hostname() {
            try {
                cachedHostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                logger.warn("Failed to resolve hostname. Using dummy-name for scheduler.");
                cachedHostname = "failed.hostname.lookup";
            }
        }
        /**
         * @see com.github.anilganipineni.scheduler.SchedulerName#getName()
         */
        @Override
        public String getName() {
            return cachedHostname;
        }
    }
}

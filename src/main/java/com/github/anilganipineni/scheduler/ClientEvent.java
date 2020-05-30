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

/**
 * @author akganipineni
 */
public class ClientEvent {
    enum EventType {
        SCHEDULE,
        RESCHEDULE,
        CANCEL
    }
    private ClientEventContext ctx;
    /**
     * @param ctx
     */
    public ClientEvent(ClientEventContext ctx) {
        this.ctx = ctx;
    }
    /**
     * @return
     */
    public ClientEventContext getContext() {
        return ctx;
    }
    /**
     * @author akganipineni
     */
    public static class ClientEventContext {
        private final EventType eventType;
        private final String taskName;
        private final String id;
        private final Object taskData;
        private final Instant executionTime;
        /**
         * @param eventType
         * @param taskName
         * @param id
         * @param taskData
         * @param executionTime
         */
        public ClientEventContext(EventType eventType, String taskName, String id, Object taskData, Instant executionTime) {
            this.eventType = eventType;
    		this.taskName = taskName;
    		this.id = id;
    		this.taskData = taskData;
            this.executionTime = executionTime;
        }
		/**
		 * @return the eventType
		 */
		public EventType getEventType() {
			return eventType;
		}
		/**
		 * @return the taskName
		 */
		public String getTaskName() {
			return taskName;
		}
		/**
		 * @return the id
		 */
		public String getId() {
			return id;
		}
		/**
		 * @return the taskData
		 */
		public Object getTaskData() {
			return taskData;
		}
		/**
		 * @return the executionTime
		 */
		public Instant getExecutionTime() {
			return executionTime;
		}
    }
}

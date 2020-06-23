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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.SchedulerState.SettableSchedulerState;
import com.github.anilganipineni.scheduler.StatsRegistry.SchedulerStatsEvent;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.dao.SchedulerRepository;
import com.github.anilganipineni.scheduler.exception.SchedulerException;
import com.github.anilganipineni.scheduler.schedule.Clock;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;

/**
 * @author akganipineni
 */
public class SchedulerImpl implements Scheduler {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(SchedulerImpl.class);

    private final SchedulerRepository<ScheduledTasks> repository;
    private final SchedulerEventListener schedulerClientEventListener;

    public static final double TRIGGER_NEXT_BATCH_WHEN_AVAILABLE_THREADS_RATIO = 0.5;
    public static final String THREAD_PREFIX = "db-scheduler";
    public static final Duration SHUTDOWN_WAIT = Duration.ofMinutes(30);
    private final Clock clock;
    
    private final TaskResolver taskResolver;
    private int threadpoolSize;
    private final ExecutorService executorService;
    private final Waiter executeDueWaiter;
    private final Duration deleteUnresolvedAfter;
    private final List<Task> initialTasks;
    private final Waiter detectDeadWaiter;
    private final Duration heartbeatInterval;
    private final StatsRegistry statsRegistry;
    private final int pollingLimit;
    private final ExecutorService taskExecutor;
    private final ExecutorService deadTaskDetector;
    private final ExecutorService eartbeatUpdator;
    private final Map<ScheduledTasks, CurrentlyExecuting> currentlyProcessing = Collections.synchronizedMap(new HashMap<>());
    private final Waiter heartbeatWaiter;
    private final SettableSchedulerState schedulerState = new SettableSchedulerState();
    private int currentGenerationNumber = 1;
    /**
     * @param clock
     * @param repository
     * @param taskResolver
     * @param threadpoolSize
     * @param executorService
     * @param schedulerName
     * @param executeDueWaiter
     * @param heartbeatInterval
     * @param enableImmediateExecution
     * @param statsRegistry
     * @param pollingLimit
     * @param deleteUnresolvedAfter
     * @param initialTasks
     */
    public SchedulerImpl(Clock clock,
    					 SchedulerRepository<ScheduledTasks> schedulerRepository,
    					 TaskResolver taskResolver,
    					 int threadpoolSize,
    					 ExecutorService executorService,
    					 SchedulerName schedulerName,
    					 Waiter executeDueWaiter,
    					 Duration heartbeatInterval,
    					 boolean enableImmediateExecution,
    					 StatsRegistry statsRegistry,
    					 int pollingLimit,
    					 Duration deleteUnresolvedAfter,
    					 List<Task> initialTasks) {

        
        SchedulerEventListener listener = (enableImmediateExecution ? new DueExecutionsListener(schedulerState, clock, executeDueWaiter) : SchedulerEventListener.NOOP);
        this.repository = schedulerRepository;
        this.schedulerClientEventListener = listener;
        
        this.clock = clock;
        this.taskResolver = taskResolver;
        this.threadpoolSize = threadpoolSize;
        this.executorService = executorService;
        this.executeDueWaiter = executeDueWaiter;
        this.deleteUnresolvedAfter = deleteUnresolvedAfter;
        this.initialTasks = initialTasks;
        this.detectDeadWaiter = new Waiter(heartbeatInterval.multipliedBy(2), clock);
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatWaiter = new Waiter(heartbeatInterval, clock);
        this.statsRegistry = statsRegistry;
        this.pollingLimit = pollingLimit;
        this.taskExecutor = Executors.newSingleThreadExecutor(ExecutorUtils.getThreadFactory(THREAD_PREFIX + "-execute-due-"));
        this.deadTaskDetector = Executors.newSingleThreadExecutor(ExecutorUtils.getThreadFactory(THREAD_PREFIX + "-detect-dead-"));
        this.eartbeatUpdator = Executors.newSingleThreadExecutor(ExecutorUtils.getThreadFactory(THREAD_PREFIX + "-update-heartbeat-"));
    }
    /**
     * @see com.github.anilganipineni.scheduler.Scheduler#start()
     */
    public void start() {
        logger.info("Starting scheduler................");
        
        executeInitialTasks();

        taskExecutor.submit(new RunUntilShutdown(this::executeDue, executeDueWaiter, schedulerState, statsRegistry));
        deadTaskDetector.submit(new RunUntilShutdown(this::detectDeadExecutions, detectDeadWaiter, schedulerState, statsRegistry));
        eartbeatUpdator.submit(new RunUntilShutdown(this::updateHeartbeats, heartbeatWaiter, schedulerState, statsRegistry));

        schedulerState.setStarted();
    }
    /**
     * Execute all initial tasks
     */
    protected void executeInitialTasks() {
        initialTasks.forEach(initialTask -> {
            try {
                initialTask.onStartup(this, this.clock);
            } catch (Exception ex) {
                logger.error("Unexpected error while executing OnStartup tasks. Continuing.", ex);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
            }
        });
    }
    /**
     * @see com.github.anilganipineni.scheduler.Scheduler#stop()
     */
    public void stop() {
    	if(schedulerState.isShuttingDown()) {
    		logger.warn("Multiple calls to 'stop()'. Scheduler is already stopping.....");
    		return;
    	}
        schedulerState.setIsShuttingDown();
        logger.info("Shutting down Scheduler..................");
        
        if (!ExecutorUtils.shutdownNowAndAwaitTermination(taskExecutor, Duration.ofSeconds(5))) {
            logger.warn("Failed to shutdown due-executor properly.");
        }
        
        if (!ExecutorUtils.shutdownNowAndAwaitTermination(deadTaskDetector, Duration.ofSeconds(5))) {
            logger.warn("Failed to shutdown detect-dead-executor properly.");
        }
        
        if (!ExecutorUtils.shutdownNowAndAwaitTermination(eartbeatUpdator, Duration.ofSeconds(5))) {
            logger.warn("Failed to shutdown update-heartbeat-executor properly.");
        }

        logger.info("Letting running executions finish. Will wait up to {}.", SHUTDOWN_WAIT);
        if (ExecutorUtils.shutdownAndAwaitTermination(executorService, SHUTDOWN_WAIT)) {
            logger.info("Scheduler stopped!");
        } else {
            logger.warn("Scheduler stopped, but some tasks did not complete. Was currently running the following executions:\n{}",
                    new ArrayList<>(currentlyProcessing.keySet()).stream().map(ScheduledTasks::toString).collect(Collectors.joining("\n")));
        }
    }
	/**
	 * @see com.github.anilganipineni.scheduler.Scheduler#schedule(com.github.anilganipineni.scheduler.dao.ScheduledTasks,
	 *      java.time.Instant)
	 */
    @Override
    public void schedule(ScheduledTasks task, Instant executionTime) {
    	task.setExecutionTime(executionTime);
        boolean created = repository.createIfNotExists(task);
        
        if (created) {
            notifyListeners(EventType.SCHEDULE, task, executionTime);
        }
    }
    /**
     * @param eventType
     * @param task
     * @param executionTime
     */
    private void notifyListeners(EventType eventType, ScheduledTasks task, Instant executionTime) {
        try {
            schedulerClientEventListener.newEvent(new EventContext(eventType, task.getTaskName(), task.getTaskId(), task.getTaskData(), executionTime));
        } catch (Exception ex) {
            logger.error("Error when notifying SchedulerEventListener.", ex);
        }
    }
    /**
     * @see com.github.anilganipineni.scheduler.Scheduler#reschedule(com.github.anilganipineni.scheduler.dao.ScheduledTasks, java.time.Instant)
     */
    @Override
    public void reschedule(ScheduledTasks task, Instant executionTime) throws SchedulerException {
        reschedule(task, executionTime, null);
    }    
    /**
     * @see com.github.anilganipineni.scheduler.Scheduler#reschedule(com.github.anilganipineni.scheduler.dao.ScheduledTasks, java.time.Instant, java.util.Map)
     */
    @Override
    public void reschedule(ScheduledTasks task, Instant executionTime, Map<String, Object> data) throws SchedulerException {
        String taskName = task.getTaskName();
        String instanceId = task.getTaskId();
        Optional<ScheduledTasks> execution = getExecution(taskName, instanceId);
        if(execution.isPresent()) {
            if(execution.get().isPicked()) {
                throw new RuntimeException(String.format("Could not reschedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
            }

            boolean success = repository.reschedule(execution.get(), executionTime, null, null, 0, data);

            if (success) {
                notifyListeners(EventType.RESCHEDULE, task, executionTime);
            }
        } else {
            throw new RuntimeException(String.format("Could not reschedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
        }
    }    
    /**
     * @see com.github.anilganipineni.scheduler.Scheduler#cancel(com.github.anilganipineni.scheduler.dao.ScheduledTasks)
     */
    @Override
    public void cancel(ScheduledTasks task) throws SchedulerException {
        String taskName = task.getTaskName();
        String instanceId = task.getTaskId();
        Optional<ScheduledTasks> execution = getExecution(taskName, instanceId);
        if(execution.isPresent()) {
            if(execution.get().isPicked()) {
                throw new SchedulerException(String.format("Could not cancel schedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
            }

            repository.remove(execution.get());
            notifyListeners(EventType.CANCEL, task, execution.get().getExecutionTime());
        } else {
            throw new SchedulerException(String.format("Could not cancel schedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
        }
    }    
    /**
     * @see com.github.anilganipineni.scheduler.Scheduler#getScheduledExecutions(java.util.function.Consumer)
     */
    @Override
    public void getScheduledExecutions(Consumer<ScheduledTasks> consumer) throws SchedulerException {
		repository.getScheduledExecutions(consumer);
    }
    /**
     * @see com.github.anilganipineni.scheduler.Scheduler#getScheduledExecutionsForTask(java.lang.String, java.lang.Class, java.util.function.Consumer)
     */
    @Override    
    public void getScheduledExecutionsForTask(String taskName, Consumer<ScheduledTasks> consumer) throws SchedulerException {
		repository.getScheduledExecutions(taskName, consumer);
    }
    /**
     * @see com.github.anilganipineni.scheduler.Scheduler#getScheduledExecution(com.github.anilganipineni.scheduler.dao.ScheduledTasks)
     */
    @Override    
    public Optional<ScheduledTasks> getScheduledExecution(ScheduledTasks task) throws SchedulerException {
        return getExecution(task.getTaskName(), task.getTaskId());
    }
    /**
     * @param taskName
     * @param task
     * @return
     * @throws SchedulerException 
     */
    private Optional<ScheduledTasks> getExecution(String taskName, String task) throws SchedulerException {
    	return repository.getExecution(taskName, task);
    }

    /**
     * @param failingAtLeastFor
     * @return
     * @throws SchedulerException
     */
    public List<ScheduledTasks> getFailingExecutions(Duration failingAtLeastFor) throws SchedulerException {
        return repository.getExecutionsFailingLongerThan(failingAtLeastFor);
    }

    public boolean triggerCheckForDueExecutions() {
        return executeDueWaiter.wake();
    }

    public List<CurrentlyExecuting> getCurrentlyExecuting() {
        return new ArrayList<>(currentlyProcessing.values());
    }

    public void executeDue() {
		try {
	        Instant now = clock.now();
	        List<ScheduledTasks> dueExecutions = repository.getDue(now, pollingLimit);
	        logger.trace("Found {} taskinstances due for execution", dueExecutions.size());

	        int thisGenerationNumber = this.currentGenerationNumber + 1;
	        DueExecutionsBatch newDueBatch = new DueExecutionsBatch(SchedulerImpl.this.threadpoolSize, thisGenerationNumber, dueExecutions.size(), pollingLimit == dueExecutions.size());

	        for (ScheduledTasks task : dueExecutions) {
	            executorService.execute(new PickAndExecute(task, newDueBatch));
	        }
	        this.currentGenerationNumber = thisGenerationNumber;
	        statsRegistry.register(SchedulerStatsEvent.RAN_EXECUTE_DUE);
		} catch (SchedulerException ex) {
			logger.error("Failed to execute due tasks!", ex);
			// Do not throw from here
		}
    }

    public void detectDeadExecutions() {
        try {
            logger.debug("Deleting executions with unresolved tasks.");
            List<String> taskNames = taskResolver.getUnresolvedTaskNames(deleteUnresolvedAfter);
            for(String taskName : taskNames) {
                logger.warn("Deleting all executions for task with name '{}'. They have been unresolved for more than {}", taskName, deleteUnresolvedAfter);
                int removed = repository.removeExecutions(taskName);
                logger.info("Removed {} executions", removed);
                taskResolver.clearUnresolved(taskName);	
            }
			
		} catch (SchedulerException ex) {
			logger.error("Failed to detect dead tasks!", ex);
			// Do not throw from here
		}

        logger.debug("Checking for dead executions.");
        Instant now = clock.now();
        final Instant oldAgeLimit = now.minus(getMaxAgeBeforeConsideredDead());
        List<ScheduledTasks> oldExecutions = null;
		try {
			oldExecutions = repository.getDeadExecutions(oldAgeLimit);
		} catch (SchedulerException ex) {
			logger.warn("Failed to fetch the dead executions", ex);
			/* NO-OP */
		}

        if (oldExecutions == null || oldExecutions.isEmpty()) {
            logger.trace("No dead executions found.");
        } else {
            oldExecutions.forEach(execution -> {

                logger.info("Found dead execution. Delegating handling to task. ScheduledTasks: " + execution);
                try {

                    Optional<Task> task = taskResolver.resolve(execution.getTaskName());
                    if (task.isPresent()) {
                        statsRegistry.register(SchedulerStatsEvent.DEAD_EXECUTION);
                        task.get().getDeadExecutionHandler().deadExecution(execution, new ExecutionOperations(repository, execution));
                    } else {
                        logger.error("Failed to find implementation for task with name '{}' for detected dead execution. Either delete the execution from the databaser, or add an implementation for it.", execution.getTaskName());
                    }

                } catch (Throwable e) {
                    logger.error("Failed while handling dead execution {}. Will be tried again later.", execution, e);
                    statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                }
            });
        }
        statsRegistry.register(SchedulerStatsEvent.RAN_DETECT_DEAD);
    }

    void updateHeartbeats() {
        if (currentlyProcessing.isEmpty()) {
            logger.trace("No executions to update heartbeats for. Skipping.");
            return;
        }

        logger.debug("Updating heartbeats for {} executions being processed.", currentlyProcessing.size());
        Instant now = clock.now();
        new ArrayList<>(currentlyProcessing.keySet()).forEach(execution -> {
            logger.trace("Updating heartbeat for execution: " + execution);
            try {
                repository.updateHeartbeat(execution, now);
            } catch (Throwable e) {
                logger.error("Failed while updating heartbeat for execution {}. Will try again later.", execution, e);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
            }
        });
        statsRegistry.register(SchedulerStatsEvent.RAN_UPDATE_HEARTBEATS);
    }

    private Duration getMaxAgeBeforeConsideredDead() {
        return heartbeatInterval.multipliedBy(4);
    }

    private class PickAndExecute implements Runnable {
        private ScheduledTasks candidate;
        private DueExecutionsBatch addedDueExecutionsBatch;

        public PickAndExecute(ScheduledTasks candidate, DueExecutionsBatch dueExecutionsBatch) {
            this.candidate = candidate;
            this.addedDueExecutionsBatch = dueExecutionsBatch;
        }

        
        public void run() {
            if(schedulerState.isShuttingDown()) {
                logger.info("Scheduler has been shutdown. Skipping fetched due execution: " + candidate.getTaskName() + "_" + candidate.getTaskId());
                return;
            }

            if (addedDueExecutionsBatch.isOlderGenerationThan(currentGenerationNumber)) {
                // skipping execution due to it being stale
                addedDueExecutionsBatch.markBatchAsStale();
                statsRegistry.register(StatsRegistry.CandidateStatsEvent.STALE);
                logger.trace("Skipping queued execution (current generationNumber: {}, execution generationNumber: {})", currentGenerationNumber, addedDueExecutionsBatch.getGenerationNumber());
                return;
            }

            Optional<ScheduledTasks> pickedExecution;
			try {
				pickedExecution = repository.pick(candidate, clock.now());
			} catch (SchedulerException ex) {
				throw new IllegalStateException(ex.getMessage(), ex);
			}

            if (!pickedExecution.isPresent()) {
                // someone else picked id
                logger.debug("ScheduledTasks picked by another scheduler. Continuing to next due execution.");
                statsRegistry.register(StatsRegistry.CandidateStatsEvent.ALREADY_PICKED);
                return;
            }

            currentlyProcessing.put(pickedExecution.get(), new CurrentlyExecuting(clock));
            try {
                statsRegistry.register(StatsRegistry.CandidateStatsEvent.EXECUTED);
                executePickedExecution(pickedExecution.get());
            } finally {
                if (currentlyProcessing.remove(pickedExecution.get()) == null) {
                    // May happen in rare circumstances (typically concurrency tests)
                    logger.warn("Released execution was not found in collection of executions currently being processed. Should never happen.");
                }
                addedDueExecutionsBatch.oneExecutionDone(() -> triggerCheckForDueExecutions());
            }
        }

        private void executePickedExecution(ScheduledTasks execution) {
            final Optional<Task> task = taskResolver.resolve(execution.getTaskName());
            if (!task.isPresent()) {
                logger.error("Failed to find implementation for task with name '{}'. Should have been excluded in JdbcRepository.", execution.getTaskName());
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                return;
            }

            Instant executionStarted = clock.now();
            try {
                logger.debug("Executing " + execution);
                CompletionHandler completion = task.get().execute(execution, new ExecutionContext(schedulerState, execution, SchedulerImpl.this));
                logger.debug("ScheduledTasks done");

                complete(completion, execution, executionStarted);
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.COMPLETED);

            } catch (RuntimeException unhandledException) {
                logger.error("Unhandled exception during execution of task with name '{}'. Treating as failure.", task.get().getName(), unhandledException);
                failure(task.get().getFailureHandler(), execution, unhandledException, executionStarted);
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.FAILED);

            } catch (Throwable unhandledError) {
                logger.error("Error during execution of task with name '{}'. Treating as failure.", task.get().getName(), unhandledError);
                failure(task.get().getFailureHandler(), execution, unhandledError, executionStarted);
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.FAILED);
            }
        }

        private void complete(CompletionHandler completion, ScheduledTasks execution, Instant executionStarted) {
            ExecutionComplete completeEvent = ExecutionComplete.success(execution, executionStarted, clock.now());
            try {
                completion.complete(completeEvent, new ExecutionOperations(repository, execution));
                statsRegistry.registerSingleCompletedExecution(completeEvent);
            } catch (Throwable e) {
                statsRegistry.register(SchedulerStatsEvent.COMPLETIONHANDLER_ERROR);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                logger.error("Failed while completing execution {}. ScheduledTasks will likely remain scheduled and locked/picked. " +
                        "The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, getMaxAgeBeforeConsideredDead(), e);
            }
        }

        private void failure(FailureHandler failureHandler, ScheduledTasks execution, Throwable cause, Instant executionStarted) {
            ExecutionComplete completeEvent = ExecutionComplete.failure(execution, executionStarted, clock.now(), cause);
            try {
                failureHandler.onFailure(completeEvent, new ExecutionOperations(repository, execution));
                statsRegistry.registerSingleCompletedExecution(completeEvent);
            } catch (Throwable e) {
                statsRegistry.register(SchedulerStatsEvent.FAILUREHANDLER_ERROR);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                logger.error("Failed while completing execution {}. ScheduledTasks will likely remain scheduled and locked/picked. " +
                        "The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, getMaxAgeBeforeConsideredDead(), e);
            }
        }

    }
    /**
     * @param task
     */
    public void addTask(Task task) {
    	taskResolver.addTask(task);
    }
}

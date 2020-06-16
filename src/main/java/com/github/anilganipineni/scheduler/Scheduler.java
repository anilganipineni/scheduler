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

import static com.github.anilganipineni.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.dao.SchedulerRepository;
import com.github.anilganipineni.scheduler.exception.SchedulerException;
import com.github.anilganipineni.scheduler.schedule.Clock;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;

public class Scheduler implements SchedulerClient {

    public static final double TRIGGER_NEXT_BATCH_WHEN_AVAILABLE_THREADS_RATIO = 0.5;
    public static final String THREAD_PREFIX = "db-scheduler";
    public static final Duration SHUTDOWN_WAIT = Duration.ofMinutes(30);
    // private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger LOG = LogManager.getLogger(Scheduler.class);
    private final SchedulerClient delegate;
    private final Clock clock;
    private final SchedulerRepository<ScheduledTasks> taskRepository;
    private final TaskResolver taskResolver;
    private int threadpoolSize;
    private final ExecutorService executorService;
    private final Waiter executeDueWaiter;
    private final Duration deleteUnresolvedAfter;
    protected final List<Task> onStartup;
    private final Waiter detectDeadWaiter;
    private final Duration heartbeatInterval;
    private final StatsRegistry statsRegistry;
    private final int pollingLimit;
    private final ExecutorService dueExecutor;
    private final ExecutorService detectDeadExecutor;
    private final ExecutorService updateHeartbeatExecutor;
    private final Map<ScheduledTasks, CurrentlyExecuting> currentlyProcessing = Collections.synchronizedMap(new HashMap<>());
    private final Waiter heartbeatWaiter;
    private final SettableSchedulerState schedulerState = new SettableSchedulerState();
    private int currentGenerationNumber = 1;

    public Scheduler(Clock clock, SchedulerRepository<ScheduledTasks> taskRepository, TaskResolver taskResolver, int threadpoolSize, ExecutorService executorService, SchedulerName schedulerName,
              Waiter executeDueWaiter, Duration heartbeatInterval, boolean enableImmediateExecution, StatsRegistry statsRegistry, int pollingLimit, Duration deleteUnresolvedAfter, List<Task> onStartup) {
        this.clock = clock;
        this.taskRepository = taskRepository;
        this.taskResolver = taskResolver;
        this.threadpoolSize = threadpoolSize;
        this.executorService = executorService;
        this.executeDueWaiter = executeDueWaiter;
        this.deleteUnresolvedAfter = deleteUnresolvedAfter;
        this.onStartup = onStartup;
        this.detectDeadWaiter = new Waiter(heartbeatInterval.multipliedBy(2), clock);
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatWaiter = new Waiter(heartbeatInterval, clock);
        this.statsRegistry = statsRegistry;
        this.pollingLimit = pollingLimit;
        this.dueExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-execute-due-"));
        this.detectDeadExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-detect-dead-"));
        this.updateHeartbeatExecutor = Executors.newSingleThreadExecutor(defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-update-heartbeat-"));
        SchedulerClientEventListener earlyExecutionListener = (enableImmediateExecution ? new TriggerCheckForDueExecutions(schedulerState, clock, executeDueWaiter) : SchedulerClientEventListener.NOOP);
        delegate = new StandardSchedulerClient(taskRepository, earlyExecutionListener);
    }

    public void start() {
        LOG.info("Starting scheduler.");

        executeOnStartup();

        dueExecutor.submit(new RunUntilShutdown(this::executeDue, executeDueWaiter, schedulerState, statsRegistry));
        detectDeadExecutor.submit(new RunUntilShutdown(this::detectDeadExecutions, detectDeadWaiter, schedulerState, statsRegistry));
        updateHeartbeatExecutor.submit(new RunUntilShutdown(this::updateHeartbeats, heartbeatWaiter, schedulerState, statsRegistry));

        schedulerState.setStarted();
    }

    protected void executeOnStartup() {
        onStartup.forEach(os -> {
            try {
                os.onStartup(this, this.clock);
            } catch (Exception e) {
                LOG.error("Unexpected error while executing OnStartup tasks. Continuing.", e);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
            }
        });
    }

    public void stop() {
        if (schedulerState.isShuttingDown()) {
            LOG.warn("Multiple calls to 'stop()'. Scheduler is already stopping.");
            return;
        }

        schedulerState.setIsShuttingDown();

        LOG.info("Shutting down Scheduler.");
        if (!ExecutorUtils.shutdownNowAndAwaitTermination(dueExecutor, Duration.ofSeconds(5))) {
            LOG.warn("Failed to shutdown due-executor properly.");
        }
        if (!ExecutorUtils.shutdownNowAndAwaitTermination(detectDeadExecutor, Duration.ofSeconds(5))) {
            LOG.warn("Failed to shutdown detect-dead-executor properly.");
        }
        if (!ExecutorUtils.shutdownNowAndAwaitTermination(updateHeartbeatExecutor, Duration.ofSeconds(5))) {
            LOG.warn("Failed to shutdown update-heartbeat-executor properly.");
        }

        LOG.info("Letting running executions finish. Will wait up to {}.", SHUTDOWN_WAIT);
        if (ExecutorUtils.shutdownAndAwaitTermination(executorService, SHUTDOWN_WAIT)) {
            LOG.info("Scheduler stopped.");
        } else {
            LOG.warn("Scheduler stopped, but some tasks did not complete. Was currently running the following executions:\n{}",
                    new ArrayList<>(currentlyProcessing.keySet()).stream().map(ScheduledTasks::toString).collect(Collectors.joining("\n")));
        }
    }

    public SchedulerState getSchedulerState() {
        return schedulerState;
    }

    
    public <T> void schedule(ScheduledTasks taskId, Instant executionTime) {
        this.delegate.schedule(taskId, executionTime);
    }

    
    public void reschedule(ScheduledTasks task, Instant newExecutionTime) {
        this.delegate.reschedule(task, newExecutionTime);
    }

    
    public void reschedule(ScheduledTasks task, Instant newExecutionTime, Map<String, Object> newData) {
        this.delegate.reschedule(task, newExecutionTime, newData);
    }

    
    public void cancel(ScheduledTasks task) {
        this.delegate.cancel(task);
    }

    
    public void getScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer) {
        this.delegate.getScheduledExecutions(consumer);
    }

    
    public <T> void getScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer) {
        this.delegate.getScheduledExecutionsForTask(taskName, dataClass, consumer);
    }

    
    public Optional<ScheduledExecution<Object>> getScheduledExecution(ScheduledTasks task) {
        return this.delegate.getScheduledExecution(task);
    }

    /**
     * @param failingAtLeastFor
     * @return
     * @throws SchedulerException
     */
    public List<ScheduledTasks> getFailingExecutions(Duration failingAtLeastFor) throws SchedulerException {
        return taskRepository.getExecutionsFailingLongerThan(failingAtLeastFor);
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
	        List<ScheduledTasks> dueExecutions = taskRepository.getDue(now, pollingLimit);
	        LOG.trace("Found {} taskinstances due for execution", dueExecutions.size());

	        int thisGenerationNumber = this.currentGenerationNumber + 1;
	        DueExecutionsBatch newDueBatch = new DueExecutionsBatch(Scheduler.this.threadpoolSize, thisGenerationNumber, dueExecutions.size(), pollingLimit == dueExecutions.size());

	        for (ScheduledTasks task : dueExecutions) {
	            executorService.execute(new PickAndExecute(task, newDueBatch));
	        }
	        this.currentGenerationNumber = thisGenerationNumber;
	        statsRegistry.register(SchedulerStatsEvent.RAN_EXECUTE_DUE);
		} catch (SchedulerException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
    }

    public void detectDeadExecutions() {
        try {
            LOG.debug("Deleting executions with unresolved tasks.");
            List<String> taskNames = taskResolver.getUnresolvedTaskNames(deleteUnresolvedAfter);
            for(String taskName : taskNames) {
                LOG.warn("Deleting all executions for task with name '{}'. They have been unresolved for more than {}", taskName, deleteUnresolvedAfter);
                int removed = taskRepository.removeExecutions(taskName);
                LOG.info("Removed {} executions", removed);
                taskResolver.clearUnresolved(taskName);	
            }
			
		} catch (SchedulerException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}

        LOG.debug("Checking for dead executions.");
        Instant now = clock.now();
        final Instant oldAgeLimit = now.minus(getMaxAgeBeforeConsideredDead());
        List<ScheduledTasks> oldExecutions = null;
		try {
			oldExecutions = taskRepository.getDeadExecutions(oldAgeLimit);
		} catch (SchedulerException ex) {
			LOG.warn("Failed to fetch the dead executions", ex);
			/* NO-OP */
		}

        if (oldExecutions == null || oldExecutions.isEmpty()) {
            LOG.trace("No dead executions found.");
        } else {
            oldExecutions.forEach(execution -> {

                LOG.info("Found dead execution. Delegating handling to task. ScheduledTasks: " + execution);
                try {

                    Optional<Task> task = taskResolver.resolve(execution.getTaskName());
                    if (task.isPresent()) {
                        statsRegistry.register(SchedulerStatsEvent.DEAD_EXECUTION);
                        task.get().getDeadExecutionHandler().deadExecution(execution, new ExecutionOperations(taskRepository, execution));
                    } else {
                        LOG.error("Failed to find implementation for task with name '{}' for detected dead execution. Either delete the execution from the databaser, or add an implementation for it.", execution.getTaskName());
                    }

                } catch (Throwable e) {
                    LOG.error("Failed while handling dead execution {}. Will be tried again later.", execution, e);
                    statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                }
            });
        }
        statsRegistry.register(SchedulerStatsEvent.RAN_DETECT_DEAD);
    }

    void updateHeartbeats() {
        if (currentlyProcessing.isEmpty()) {
            LOG.trace("No executions to update heartbeats for. Skipping.");
            return;
        }

        LOG.debug("Updating heartbeats for {} executions being processed.", currentlyProcessing.size());
        Instant now = clock.now();
        new ArrayList<>(currentlyProcessing.keySet()).forEach(execution -> {
            LOG.trace("Updating heartbeat for execution: " + execution);
            try {
                taskRepository.updateHeartbeat(execution, now);
            } catch (Throwable e) {
                LOG.error("Failed while updating heartbeat for execution {}. Will try again later.", execution, e);
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
                LOG.info("Scheduler has been shutdown. Skipping fetched due execution: " + candidate.getTaskName() + "_" + candidate.getTaskId());
                return;
            }

            if (addedDueExecutionsBatch.isOlderGenerationThan(currentGenerationNumber)) {
                // skipping execution due to it being stale
                addedDueExecutionsBatch.markBatchAsStale();
                statsRegistry.register(StatsRegistry.CandidateStatsEvent.STALE);
                LOG.trace("Skipping queued execution (current generationNumber: {}, execution generationNumber: {})", currentGenerationNumber, addedDueExecutionsBatch.getGenerationNumber());
                return;
            }

            Optional<ScheduledTasks> pickedExecution;
			try {
				pickedExecution = taskRepository.pick(candidate, clock.now());
			} catch (SchedulerException ex) {
				throw new IllegalStateException(ex.getMessage(), ex);
			}

            if (!pickedExecution.isPresent()) {
                // someone else picked id
                LOG.debug("ScheduledTasks picked by another scheduler. Continuing to next due execution.");
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
                    LOG.warn("Released execution was not found in collection of executions currently being processed. Should never happen.");
                }
                addedDueExecutionsBatch.oneExecutionDone(() -> triggerCheckForDueExecutions());
            }
        }

        private void executePickedExecution(ScheduledTasks execution) {
            final Optional<Task> task = taskResolver.resolve(execution.getTaskName());
            if (!task.isPresent()) {
                LOG.error("Failed to find implementation for task with name '{}'. Should have been excluded in JdbcRepository.", execution.getTaskName());
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                return;
            }

            Instant executionStarted = clock.now();
            try {
                LOG.debug("Executing " + execution);
                CompletionHandler completion = task.get().execute(execution, new ExecutionContext(schedulerState, execution, Scheduler.this));
                LOG.debug("ScheduledTasks done");

                complete(completion, execution, executionStarted);
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.COMPLETED);

            } catch (RuntimeException unhandledException) {
                LOG.error("Unhandled exception during execution of task with name '{}'. Treating as failure.", task.get().getName(), unhandledException);
                failure(task.get().getFailureHandler(), execution, unhandledException, executionStarted);
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.FAILED);

            } catch (Throwable unhandledError) {
                LOG.error("Error during execution of task with name '{}'. Treating as failure.", task.get().getName(), unhandledError);
                failure(task.get().getFailureHandler(), execution, unhandledError, executionStarted);
                statsRegistry.register(StatsRegistry.ExecutionStatsEvent.FAILED);
            }
        }

        private void complete(CompletionHandler completion, ScheduledTasks execution, Instant executionStarted) {
            ExecutionComplete completeEvent = ExecutionComplete.success(execution, executionStarted, clock.now());
            try {
                completion.complete(completeEvent, new ExecutionOperations(taskRepository, execution));
                statsRegistry.registerSingleCompletedExecution(completeEvent);
            } catch (Throwable e) {
                statsRegistry.register(SchedulerStatsEvent.COMPLETIONHANDLER_ERROR);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                LOG.error("Failed while completing execution {}. ScheduledTasks will likely remain scheduled and locked/picked. " +
                        "The execution should be detected as dead in {}, and handled according to the tasks DeadExecutionHandler.", execution, getMaxAgeBeforeConsideredDead(), e);
            }
        }

        private void failure(FailureHandler failureHandler, ScheduledTasks execution, Throwable cause, Instant executionStarted) {
            ExecutionComplete completeEvent = ExecutionComplete.failure(execution, executionStarted, clock.now(), cause);
            try {
                failureHandler.onFailure(completeEvent, new ExecutionOperations(taskRepository, execution));
                statsRegistry.registerSingleCompletedExecution(completeEvent);
            } catch (Throwable e) {
                statsRegistry.register(SchedulerStatsEvent.FAILUREHANDLER_ERROR);
                statsRegistry.register(SchedulerStatsEvent.UNEXPECTED_ERROR);
                LOG.error("Failed while completing execution {}. ScheduledTasks will likely remain scheduled and locked/picked. " +
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
    /**
     * @param dataSource
     * @param knownTasks
     * @return
     */
    public static SchedulerBuilder create(SchedulerDataSource dataSource, Task ... knownTasks) {
        return create(dataSource, Arrays.asList(knownTasks));
    }
    /**
     * @param dataSource
     * @param knownTasks
     * @return
     */
    public static SchedulerBuilder create(SchedulerDataSource dataSource, List<Task> knownTasks) {
        return new SchedulerBuilder(dataSource, knownTasks);
    }
}

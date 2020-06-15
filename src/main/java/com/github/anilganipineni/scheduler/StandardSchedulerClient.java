package com.github.anilganipineni.scheduler;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.dao.SchedulerRepository;
import com.github.anilganipineni.scheduler.exception.SchedulerException;

/**
 * @author akganipineni
 */
public class StandardSchedulerClient implements SchedulerClient {

    private static final Logger LOG = LoggerFactory.getLogger(StandardSchedulerClient.class);
    protected final SchedulerRepository<ScheduledTasks> taskRepository;
    private SchedulerClientEventListener schedulerClientEventListener;

    StandardSchedulerClient(SchedulerRepository<ScheduledTasks> taskRepository) {
        this(taskRepository, SchedulerClientEventListener.NOOP);
    }

    StandardSchedulerClient(SchedulerRepository<ScheduledTasks> taskRepository, SchedulerClientEventListener schedulerClientEventListener) {
        this.taskRepository = taskRepository;
        this.schedulerClientEventListener = schedulerClientEventListener;
    }

    @Override
    public <T> void schedule(ScheduledTasks taskId, Instant executionTime) {
        boolean success = taskRepository.createIfNotExists(new ScheduledTasks(executionTime, taskId.getTaskName(), taskId.getTaskId(), taskId.getTaskData()));
        if (success) {
            notifyListeners(ClientEvent.EventType.SCHEDULE, taskId, executionTime);
        }
    }

    @Override
    public void reschedule(ScheduledTasks task, Instant newExecutionTime) {
        reschedule(task, newExecutionTime, null);
    }

    @Override
    public void reschedule(ScheduledTasks task, Instant newExecutionTime, Map<String, Object> newData) {
        String taskName = task.getTaskName();
        String instanceId = task.getTaskId();
        Optional<ScheduledTasks> execution = getExecution(taskName, instanceId);
        if(execution.isPresent()) {
            if(execution.get().isPicked()) {
                throw new RuntimeException(String.format("Could not reschedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
            }

            boolean success;
            if (newData == null) {
                success = taskRepository.reschedule(execution.get(), newExecutionTime, null, null, 0);
            } else {
                success = taskRepository.reschedule(execution.get(), newExecutionTime, null, null, 0, newData);
            }

            if (success) {
                notifyListeners(ClientEvent.EventType.RESCHEDULE, task, newExecutionTime);
            }
        } else {
            throw new RuntimeException(String.format("Could not reschedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
        }
    }

    @Override
    public void cancel(ScheduledTasks task) {
        String taskName = task.getTaskName();
        String instanceId = task.getTaskId();
        Optional<ScheduledTasks> execution = getExecution(taskName, instanceId);
        if(execution.isPresent()) {
            if(execution.get().isPicked()) {
                throw new RuntimeException(String.format("Could not cancel schedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
            }

            taskRepository.remove(execution.get());
            notifyListeners(ClientEvent.EventType.CANCEL, task, execution.get().getExecutionTime());
        } else {
            throw new RuntimeException(String.format("Could not cancel schedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
        }
    }

    @Override
    public void getScheduledExecutions(Consumer<ScheduledExecution<Object>> consumer) {
        try {
			taskRepository.getScheduledExecutions(execution -> consumer.accept(new ScheduledExecution<>(Object.class, execution)));
		} catch (SchedulerException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
    }

    @Override
    public <T> void getScheduledExecutionsForTask(String taskName, Class<T> dataClass, Consumer<ScheduledExecution<T>> consumer) {
        try {
			taskRepository.getScheduledExecutions(taskName, execution -> consumer.accept(new ScheduledExecution<>(dataClass, execution)));
		} catch (SchedulerException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
    }

    @Override
    public Optional<ScheduledExecution<Object>> getScheduledExecution(ScheduledTasks task) {
        Optional<ScheduledTasks> e = getExecution(task.getTaskName(), task.getTaskId());
        return e.map(oe -> new ScheduledExecution<>(Object.class, oe));
    }

    private void notifyListeners(ClientEvent.EventType eventType, ScheduledTasks task, Instant executionTime) {
        try {
            schedulerClientEventListener.newEvent(new ClientEvent(new ClientEvent.ClientEventContext(eventType, task.getTaskName(), task.getTaskId(), task.getTaskData(), executionTime)));
        } catch (Exception e) {
            LOG.error("Error when notifying SchedulerClientEventListener.", e);
        }
    }
    /**
     * @param taskName
     * @param task
     * @return
     */
    public Optional<ScheduledTasks> getExecution(String taskName, String task) {
    	try {
			return taskRepository.getExecution(taskName, task);
		} catch (SchedulerException ex) {
            throw new RuntimeException(ex.getMessage(), ex); // TODO : Why runtime exception
		}
    }


}

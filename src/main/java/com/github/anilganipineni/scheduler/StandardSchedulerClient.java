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
    public <T> void schedule(ScheduledTasks taskInstance, Instant executionTime) {
        boolean success = taskRepository.createIfNotExists(new ScheduledTasks(executionTime, taskInstance.getTaskName(), taskInstance.getId(), taskInstance.getTaskData()));
        if (success) {
            notifyListeners(ClientEvent.EventType.SCHEDULE, taskInstance, executionTime);
        }
    }

    @Override
    public void reschedule(ScheduledTasks taskInstanceId, Instant newExecutionTime) {
        reschedule(taskInstanceId, newExecutionTime, null);
    }

    @Override
    public void reschedule(ScheduledTasks taskInstanceId, Instant newExecutionTime, Map<String, Object> newData) {
        String taskName = taskInstanceId.getTaskName();
        String instanceId = taskInstanceId.getId();
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
                notifyListeners(ClientEvent.EventType.RESCHEDULE, taskInstanceId, newExecutionTime);
            }
        } else {
            throw new RuntimeException(String.format("Could not reschedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
        }
    }

    @Override
    public void cancel(ScheduledTasks taskInstanceId) {
        String taskName = taskInstanceId.getTaskName();
        String instanceId = taskInstanceId.getId();
        Optional<ScheduledTasks> execution = getExecution(taskName, instanceId);
        if(execution.isPresent()) {
            if(execution.get().isPicked()) {
                throw new RuntimeException(String.format("Could not cancel schedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
            }

            taskRepository.remove(execution.get());
            notifyListeners(ClientEvent.EventType.CANCEL, taskInstanceId, execution.get().executionTime);
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
    public Optional<ScheduledExecution<Object>> getScheduledExecution(ScheduledTasks taskInstanceId) {
        Optional<ScheduledTasks> e = getExecution(taskInstanceId.getTaskName(), taskInstanceId.getId());
        return e.map(oe -> new ScheduledExecution<>(Object.class, oe));
    }

    private void notifyListeners(ClientEvent.EventType eventType, ScheduledTasks taskInstanceId, Instant executionTime) {
        try {
            schedulerClientEventListener.newEvent(new ClientEvent(new ClientEvent.ClientEventContext(eventType, taskInstanceId.getTaskName(), taskInstanceId.getId(), taskInstanceId.getTaskData(), executionTime)));
        } catch (Exception e) {
            LOG.error("Error when notifying SchedulerClientEventListener.", e);
        }
    }
    /**
     * @param taskName
     * @param taskInstanceId
     * @return
     */
    public Optional<ScheduledTasks> getExecution(String taskName, String taskInstanceId) {
    	try {
			return taskRepository.getExecution(taskName, taskInstanceId);
		} catch (SchedulerException ex) {
            throw new RuntimeException(ex.getMessage(), ex); // TODO : Why runtime exception
		}
    }


}

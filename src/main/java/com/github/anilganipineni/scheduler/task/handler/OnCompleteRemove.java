package com.github.anilganipineni.scheduler.task.handler;

import com.github.anilganipineni.scheduler.task.CompletionHandler;
import com.github.anilganipineni.scheduler.task.helper.ExecutionComplete;
import com.github.anilganipineni.scheduler.task.helper.ExecutionOperations;

/**
 * @author akganipineni
 */
public class OnCompleteRemove implements CompletionHandler {
	/**
	 * @see com.github.anilganipineni.scheduler.task.CompletionHandler#complete(com.github.anilganipineni.scheduler.task.helper.ExecutionComplete,
	 *      com.github.anilganipineni.scheduler.task.helper.ExecutionOperations)
	 */
	@Override
	public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
		executionOperations.stop();
	}
}

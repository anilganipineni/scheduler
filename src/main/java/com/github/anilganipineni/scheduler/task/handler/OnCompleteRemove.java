package com.github.anilganipineni.scheduler.task.handler;

import com.github.anilganipineni.scheduler.ExecutionComplete;
import com.github.anilganipineni.scheduler.ExecutionOperations;

/**
 * @author akganipineni
 */
public class OnCompleteRemove implements CompletionHandler {
	/**
	 * @see com.github.anilganipineni.scheduler.task.handler.CompletionHandler#complete(com.github.anilganipineni.scheduler.ExecutionComplete,
	 *      com.github.anilganipineni.scheduler.ExecutionOperations)
	 */
	@Override
	public void complete(ExecutionComplete executionComplete, ExecutionOperations executionOperations) {
		executionOperations.stop();
	}
}

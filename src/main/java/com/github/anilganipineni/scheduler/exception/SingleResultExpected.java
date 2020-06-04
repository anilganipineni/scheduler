package com.github.anilganipineni.scheduler.exception;

/**
 * @author akganipineni
 */
public class SingleResultExpected extends SQLRuntimeException {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;

	public SingleResultExpected(String message) {
		super(message);
	}
}

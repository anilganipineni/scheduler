package com.github.anilganipineni.scheduler.exception;

import java.util.List;

/**
 * @author akganipineni
 * 
 * Base class for all custom exceptions generated in {com.adaequare.gst.gsp Project}.
 * All application routines that throw custom exceptions are supposed to throw
 * an instance (or a subclass) of this exception.
 */
public class SchedulerException extends Exception {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * The list of errors
	 */
	private List<String> m_errors = null;
	/**
	 * Construct a default instance.
	 */
	public SchedulerException() {
		super();
	}
	/**
	 * Construct an instance with specified message.
	 * 
	 * @param message
	 */
	public SchedulerException(String message) {
		super(message);
	}
	/**
	 * Construct an instance with specified cause.
	 * 
	 * @param cause
	 */
	public SchedulerException(Throwable cause) {
		super(cause);
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public SchedulerException(String message, Throwable cause) {
		super(message, cause);
	}
	/**
	 * @param errors
	 */
	public SchedulerException(List<String> errors) {
		m_errors = errors;
	}
	/**
	 * @param errors
	 * @param cause
	 */
	public SchedulerException(List<String> errors, Throwable cause) {
		super(cause);
		m_errors = errors;
	}
	/**
	 * @return the errors
	 */
	public final List<String> getErrors() {
		return m_errors;
	}
	/**
	 * @param errors the errors to set
	 */
	public final void setErrors(List<String> errors) {
		m_errors = errors;
	}
	/**
	 * Returns the detail message string of this GSP Exception if m_errors is not null.
	 * 
	 * @see java.lang.Throwable#getMessage()
	 */
	@Override
	public final String getMessage() {
		if(m_errors != null && m_errors.size() > 0) {
			StringBuilder sb = new StringBuilder();
			for(String s : m_errors) {
				sb.append(s).append(", ");
			}
			String error =  sb.toString();
			return sb.substring(0, error.length() - 2).toString();
		}
		return super.getMessage();
	}
}

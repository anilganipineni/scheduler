package com.adaequare.gst.core.exception;

import java.util.List;

/**
 * @author akganipineni
 * 
 * Base class for all custom exceptions generated in {com.adaequare.gst.gsp Project}.
 * All application routines that throw custom exceptions are supposed to throw
 * an instance (or a subclass) of this exception.
 */
public abstract class GstException extends Exception {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	public static final String SERVER_ERROR					= "1001";
	/**
	 * The list of errors
	 */
	private List<String> m_errors = null;
	/**
	 * The GWY error code used to send out with the response in case of exception
	 */
	private String m_errorCode = SERVER_ERROR;
	/**
	 * The {@link #m_est} sent by GSTN
	 */
	private Integer m_est = 0;
	/**
	 * Construct a default instance.
	 */
	public GstException() {
		super();
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public GstException(String message, Throwable cause) {
		super(message, cause);
	}
	/**
	 * Construct an instance with specified message.
	 * 
	 * @param message
	 */
	public GstException(String message) {
		super(message);
	}
	/**
	 * Construct an instance with specified cause.
	 * 
	 * @param cause
	 */
	public GstException(Throwable cause) {
		super(cause);
	}
	/**
	 * @param errors
	 */
	public GstException(List<String> errors) {
		m_errors = errors;
	}
	/**
	 * @param errors
	 * @param cause
	 */
	public GstException(List<String> errors, Throwable cause) {
		super(cause);
		m_errors = errors;
	}
	/**
	 * @param message
	 * @param errorCode
	 */
	public GstException(String message, String errorCode) {
		super(message);
		m_errorCode = errorCode;
	}
	/**
	 * @param message
	 * @param errorCode
	 * @param est
	 */
	public GstException(String message, String errorCode, Integer est) {
		super(message);
		m_errorCode = errorCode;
		m_est = est;
	}
	/**
	 * @param errors
	 * @param errorCode
	 */
	public GstException(List<String> errors, String errorCode) {
		m_errors = errors;
		m_errorCode = errorCode;
	}
	/**
	 * @return the errorCode
	 */
	public final String getErrorCode() {
		return m_errorCode;
	}
	/**
	 * @param errorCode the errorCode to set
	 */
	public final void setErrorCode(String errorCode) {
		m_errorCode = errorCode;
	}
	/**
	 * @return the est
	 */
	public final Integer getEst() {
		return m_est;
	}
	/**
	 * @param est the est to set
	 */
	public final void setEst(Integer est) {
		m_est = est;
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

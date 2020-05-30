package com.adaequare.gst.core.exception;

import java.util.List;

/**
 * @author akganipineni
 */
public class GstStandardException extends GstException {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Construct a default instance.
	 */
	public GstStandardException() {
		super();
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public GstStandardException(String message, Throwable cause) {
		super(message, cause);
	}
	/**
	 * Construct an instance with specified message.
	 * 
	 * @param message
	 */
	public GstStandardException(String message) {
		super(message);
	}
	/**
	 * Construct an instance with specified cause.
	 * 
	 * @param cause
	 */
	public GstStandardException(Throwable cause) {
		super(cause);
	}
	/**
	 * @param errors
	 */
	public GstStandardException(List<String> errors) {
		super(errors);
	}
	/**
	 * @param errors
	 * @param cause
	 */
	public GstStandardException(List<String> errors, Throwable cause) {
		super(errors, cause);
	}
	/**
	 * @param errors
	 * @param errorCode
	 */
	public GstStandardException(List<String> errors, String errorCode) {
		super(errors, errorCode);
	}
	/**
	 * @param message
	 * @param errorCode
	 * @param est
	 */
	public GstStandardException(String message, String errorCode, Integer est) {
		super(message, errorCode, est);
	}
	/**
	 * @param message
	 * @param errorCode
	 */
	public GstStandardException(String message, String errorCode) {
		super(message, errorCode);
	}
}
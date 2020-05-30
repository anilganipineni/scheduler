package com.adaequare.gst.core.exception;

/**
 * @author akganipineni
 */
public class GstAuthorizationException extends GstException {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Construct an instance with specified message.
	 * 
	 * @param message
	 */
	public GstAuthorizationException(String message) {
		super(message);
	}
	/**
	 * @param message
	 * @param errorCode
	 */
	public GstAuthorizationException(String message, String errorCode) {
		super(message, errorCode);
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public GstAuthorizationException(String message, Throwable cause) {
		super(message, cause);
	}
}
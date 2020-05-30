package com.adaequare.gst.core.exception;

/**
 * @author akganipineni
 */
public class GstDataBaseException extends GstException {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Construct a default instance.
	 */
	public GstDataBaseException() {
		super();
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public GstDataBaseException(String message, Throwable cause) {
		super(message, cause);
	}
	/**
	 * Construct an instance with specified message.
	 * 
	 * @param message
	 */
	public GstDataBaseException(String message) {
		super(message);
	}
	/**
	 * Construct an instance with specified cause.
	 * 
	 * @param cause
	 */
	public GstDataBaseException(Throwable cause) {
		super(cause);
	}
}
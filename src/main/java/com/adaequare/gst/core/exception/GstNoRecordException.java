package com.adaequare.gst.core.exception;

/**
 * @author akganipineni
 */
public class GstNoRecordException extends GstDataBaseException {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Construct a default instance.
	 */
	public GstNoRecordException() {
		super();
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public GstNoRecordException(String message, Throwable cause) {
		super(message, cause);
	}
	/**
	 * Construct an instance with specified message.
	 * 
	 * @param message
	 */
	public GstNoRecordException(String message) {
		super(message);
	}
	/**
	 * Construct an instance with specified cause.
	 * 
	 * @param cause
	 */
	public GstNoRecordException(Throwable cause) {
		super(cause);
	}
}
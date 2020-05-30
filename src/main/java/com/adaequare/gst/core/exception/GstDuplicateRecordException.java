package com.adaequare.gst.core.exception;

/**
 * @author akganipineni
 */
public class GstDuplicateRecordException extends GstDataBaseException {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Construct a default instance.
	 */
	public GstDuplicateRecordException() {
		super();
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public GstDuplicateRecordException(String message, Throwable cause) {
		super(message, cause);
	}
	/**
	 * Construct an instance with specified message.
	 * 
	 * @param message
	 */
	public GstDuplicateRecordException(String message) {
		super(message);
	}
	/**
	 * Construct an instance with specified cause.
	 * 
	 * @param cause
	 */
	public GstDuplicateRecordException(Throwable cause) {
		super(cause);
	}
}
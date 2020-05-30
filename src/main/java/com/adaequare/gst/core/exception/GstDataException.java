package com.adaequare.gst.core.exception;

public class GstDataException extends GstException {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Construct a default instance.
	 */
	public GstDataException() {
		super();
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public GstDataException(String message, Throwable cause) {
		super(message, cause);
	}
	/**
	 * Construct an instance with specified message.
	 * 
	 * @param message
	 */
	public GstDataException(String message) {
		super(message);
	}
	/**
	 * Construct an instance with specified cause.
	 * 
	 * @param cause
	 */
	public GstDataException(Throwable cause) {
		super(cause);
	}
	/**
	 * @param message
	 * @param errorCode
	 */
	public GstDataException(String message, String errorCode) {
		super(message);
		setErrorCode(errorCode);
	}
	/**
	 * Construct an instance with specified message and cause.
	 * 
	 * @param message
	 * @param cause
	 */
	public GstDataException(String message, String errorCode, Throwable cause) {
		super(message, cause);
		setErrorCode(errorCode);
	}
}

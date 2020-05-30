package com.adaequare.gst.core.exception;

import java.util.List;

/**
 * @author akganipineni
 */
public class GstValidationException extends GstException {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * @param errors
	 * @param errorCode
	 */
	public GstValidationException(List<String> errors, String errorCode) {
		super(errors, errorCode);
	}
}
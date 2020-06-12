package com.github.anilganipineni.scheduler.exception;

import java.util.List;

/**
 * @author akganipineni
 */
public class UnrecognizableSchedule extends RuntimeException {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UnrecognizableSchedule(String inputSchedule) {
        super("Unrecognized schedule: '" + inputSchedule + "'");
    }

    public UnrecognizableSchedule(String inputSchedule, List<String> examples) {
        super("Unrecognized schedule: '" + inputSchedule + "'. Parsable examples: " + examples);
    }
}

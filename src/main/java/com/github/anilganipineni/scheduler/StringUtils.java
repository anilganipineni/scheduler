/**
 * Copyright (C) Anil Ganipineni
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.anilganipineni.scheduler;

import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author akganipineni
 */
public final class StringUtils {
    /**
     * The empty String {@code ""}.
     * @since 2.0
     */
	public static final String EMPTY = "";
    /**
     * <p>Checks if a CharSequence is empty (""), null or whitespace only.</p>
     *
     * <p>Whitespace is defined by {@link Character#isWhitespace(char)}.</p>
     *
     * <pre>
     * isBlank(null)      = true
     * isBlank("")        = true
     * isBlank(" ")       = true
     * isBlank("bob")     = false
     * isBlank("  bob  ") = false
     * </pre>
     *
     * @param cs  the CharSequence to check, may be null
     * @return {@code true} if the CharSequence is null, empty or whitespace only
     * @since 2.0
     * @since 3.0 Changed signature from isBlank(String) to isBlank(CharSequence)
     */
    public static boolean isBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }
	/**
	 * Convert a Map to a String Using Java Streams
	 * 
	 * @param map
	 * @return
	 */
	public static String convertMap2String(Map<String, Object> map) {
	    return map.entrySet().stream().map(e -> e.getKey() + ":" + format(e.getValue())).collect(Collectors.joining(",", "{", "}"));
	}
	/**
	 * @param value
	 * @return
	 */
	private static String format(Object value) {
		if(value != null) {
			if(value instanceof Date) {
				return ((Date) value).getTime() + "";
			}
			
			return value.toString();
		}
		return null;
	}
    /**
     * @param s
     * @param length
     * @return
     */
    public static String truncate(String s, int length) {
        if (s == null) {
            return null;
        }

        if (s.length() > length) {
            return s.substring(0, length);
        } else {
            return s;
        }
    }
}

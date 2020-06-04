package com.github.anilganipineni.scheduler;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * @author akganipineni
 */
public class JsonUtils {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static Logger logger = LogManager.getLogger(JsonUtils.class);
    /**
     * The <code>ObjectMapper</code> instance for GST Modules.
     */
	private static final ObjectMapper mapper = registerRequiredModules();
	/**
	 * @return
	 */
	private static ObjectMapper registerRequiredModules() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		
		SimpleModule dateModule = new SimpleModule();
		mapper.registerModule(dateModule);
		return mapper;
	}
	/**
	 * @param jsonObj
	 * @return
	 */
	public static String convertObject2Json(Object jsonObj) {
		return convertObject2Json(jsonObj, true);
	}
	/**
	 * @param jsonObj
	 * @param excludeNullValues
	 * @return
	 */
	public static String convertObject2Json(Object jsonObj, boolean excludeNullValues) {
		try {
			if(jsonObj == null) {
				return StringUtils.EMPTY;
			}
			
			if(excludeNullValues) {
				return mapper.writeValueAsString(jsonObj);
			}

			ObjectMapper tempMapper = new ObjectMapper();
			return tempMapper.writeValueAsString(jsonObj);
			
		} catch (IOException ex) {
			logger.error("Exception while converting Object to Json!", ex);
			return StringUtils.EMPTY;
		}
	}
	/**
	 * @param source
	 * @param targetType
	 * @return
	 */
	public static <T> T convertObject2Object(Object source, Class<T> targetType) {
		T obj = null;
		try {
			if(source != null) {
				obj = mapper.convertValue(source, targetType);
			}
		} catch (Exception ex) {
			logger.error("Failed to parse : " + source, ex);
		}
		return obj;
	}
	/**
	 * @param json
	 * @param objectClass
	 * @return
	 * @throws GstStandardException 
	 */
	public static <T> T convertJson2Object(String json, Class<T> objectClass) throws SchedulerException{
		try {
			if(StringUtils.isBlank(json)){
				return null;
			}
			ObjectReader reader = mapper.readerFor(objectClass);
			return reader.readValue(json);
		} catch (IOException e) {
			logger.error("Failed to convert Json : [{}] to Response Type[{}]", json, objectClass, e);
			throw new SchedulerException("Exception while converting Json to Map", e);
		}
	}
	/**
	 * @param json
	 * @return
	 * @throws SchedulerException
	 */
	public static <T> Map<String, T> convertJsonToMap(String json) throws SchedulerException {
		if(StringUtils.isBlank(json)){
			return Collections.emptyMap();
		}
		
		try {
			return mapper.readerFor(new TypeReference<HashMap<String, T>>(){}).readValue(json);
		} catch (IOException e) {
			throw new SchedulerException("Exception while converting Json to Map", e);
		}
	}
}

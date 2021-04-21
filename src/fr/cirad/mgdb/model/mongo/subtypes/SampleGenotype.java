/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2019, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
 package fr.cirad.mgdb.model.mongo.subtypes;

import java.util.HashMap;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class SampleGenotype.
 */
public class SampleGenotype
{
	
	/** The Constant FIELDNAME_GENOTYPECODE. */
	public final static String FIELDNAME_GENOTYPECODE = "gt";
	
	/** The Constant SECTION_ADDITIONAL_INFO. */
	public final static String SECTION_ADDITIONAL_INFO = "ai";
	
	/** The code. */
	@BsonProperty(FIELDNAME_GENOTYPECODE)
	@Field(FIELDNAME_GENOTYPECODE)
	private String code;
	
	/** The additional info. */
	@BsonProperty(SECTION_ADDITIONAL_INFO)
	@Field(SECTION_ADDITIONAL_INFO)
	private HashMap<String, Object> additionalInfo = null;

	/**
	 * Instantiates a new sample genotype.
	 */
	public SampleGenotype() {
		super();
		if (code != null)
			this.code = code.intern();
	}
	
	/**
	 * Instantiates a new sample genotype.
	 *
	 * @param code the code
	 */
	public SampleGenotype(String code) {
		super();
		if (code != null)
			this.code = code.intern();
	}
	
	/**
	 * Gets the code.
	 *
	 * @return the code
	 */
	public String getCode() {
		return code;
	}
	
	/**
	 * Sets the code.
	 *
	 * @param code the new code
	 */
	public void setCode(String code) {
		this.code = code.intern();
	}

	/**
	 * Gets the additional info.
	 *
	 * @return the additional info
	 */
	public HashMap<String, Object> getAdditionalInfo() {
		if (additionalInfo == null)
			additionalInfo = new HashMap<>();
		return additionalInfo;
	}

	/**
	 * Sets the additional info.
	 *
	 * @param additionalInfo the additional info
	 */
	public void setAdditionalInfo(HashMap<String, Object> additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
}

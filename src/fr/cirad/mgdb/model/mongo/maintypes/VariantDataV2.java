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
package fr.cirad.mgdb.model.mongo.maintypes;

import org.apache.log4j.Logger;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
//import org.springframework.data.mongodb.core.index.CompoundIndex;
//import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
//import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;

/**
 * The Class VariantData.
 */
@Document(collection = "variants")
@TypeAlias("VD")
//@CompoundIndexes({
//    @CompoundIndex(def = "{'" + VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE + "': 1, '" + VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE + "': 1}")
//})
public class VariantDataV2 extends AbstractVariantData
{
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantDataV2.class);
	
	/** The id. */
	@Id
	protected String id;
	
	/** The Constant FIELDNAME_VERSION. */
	public final static String FIELDNAME_VERSION = "v";

	/** The version. */
	@Version
	@Field(FIELDNAME_VERSION)
    private Long version;

	/**
	 * Instantiates a new variant data.
	 */
	public VariantDataV2() {
		super();
	}
	
	/**
	 * Instantiates a new variant data.
	 *
	 * @param id the id
	 */
	public VariantDataV2(String id) {
		super();
		this.id = id;
	}
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public String getId() {
		return (String) id;
	}

	/**
	 * Gets the version.
	 *
	 * @return the version
	 */
	public Long getVersion() {
		return version;
	}

	/**
	 * Sets the version.
	 *
	 * @param version the new version
	 */
	public void setVersion(Long version) {
		this.version = version;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof VariantDataV2))
			return false;
		
		return getId().equals(((VariantDataV2)o).getId());
	}
	
	@Override
	public int hashCode()	// thanks to this overriding, HashSet.contains will find such objects based on their ID
	{
		if (getId() == null)
			return super.hashCode();

		return getId().hashCode();
	}
}
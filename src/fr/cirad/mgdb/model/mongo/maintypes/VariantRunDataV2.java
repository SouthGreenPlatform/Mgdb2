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

import java.util.HashMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
//import org.springframework.data.mongodb.core.index.CompoundIndex;
//import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;

/**
 * The Class VariantRunData.
 */
@Document(collection = "variantRunData")
@TypeAlias("R")

public class VariantRunDataV2 extends AbstractVariantData
{
	/** The Constant FIELDNAME_SAMPLEGENOTYPES. */
	public final static String FIELDNAME_SAMPLEGENOTYPES = "sp";
	
	/** The Constant FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME. */
	public final static String FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME = "EFF_nm";
	
	/** The Constant FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE. */
	public final static String FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE = "EFF_ge";

	/**
	 * The Class VariantRunDataId.
	 */
	static public class VariantRunDataId
	{
		/** The Constant FIELDNAME_PROJECT_ID. */
		public final static String FIELDNAME_PROJECT_ID = "pi";
		
		/** The Constant FIELDNAME_RUNNAME. */
		public final static String FIELDNAME_RUNNAME = "rn";
		
		/** The Constant FIELDNAME_VARIANT_ID. */
		public final static String FIELDNAME_VARIANT_ID = "vi";

		/** The project id. */
		@Field(FIELDNAME_PROJECT_ID)
		private int projectId;

		/** The run name. */
		@Field(FIELDNAME_RUNNAME)
		private String runName;

		/** The variant id. */
		@Field(FIELDNAME_VARIANT_ID)
		private String variantId;

		/**
		 * Instantiates a new variant run data id.
		 *
		 * @param projectId the project id
		 * @param runName the run name
		 * @param variantId the variant id
		 */
		public VariantRunDataId(int projectId, String runName, String variantId) {
			this.projectId = projectId;
			this.runName = runName.intern();
			this.variantId = variantId;
		}

		/**
		 * Gets the project id.
		 *
		 * @return the project id
		 */
		public int getProjectId() {
			return projectId;
		}

		/**
		 * Gets the run name.
		 *
		 * @return the run name
		 */
		public String getRunName() {
			return runName;
		}

		/**
		 * Gets the variant id.
		 *
		 * @return the variant id
		 */
		public String getVariantId() {
			return variantId;
		}
		
		@Override
		public boolean equals(Object o)	// thanks to this overriding, HashSet.contains will find such objects based on their ID
		{
			if (this == o)
				return true;
			
			if (o == null || !(o instanceof VariantRunDataId))
				return false;
			
			return getProjectId() == ((VariantRunDataId)o).getProjectId() && getRunName().equals(((VariantRunDataId)o).getRunName()) && getVariantId().equals(((VariantRunDataId)o).getVariantId());
		}

		@Override
		public int hashCode()	// thanks to this overriding, HashSet.contains will find such objects based on their ID
		{
			return toString().hashCode();
		}
		
		@Override
		public String toString()
		{
			return projectId + "§" + runName + "§" + variantId;
		}
	}

	/** The id. */
	@Id
	private VariantRunDataId id;

	/** The sample genotypes. */
	@Field(FIELDNAME_SAMPLEGENOTYPES)
	private HashMap<Integer, SampleGenotype> sampleGenotypes = new HashMap<Integer, SampleGenotype>();

	/**
	 * Instantiates a new variant run data.
	 */
	public VariantRunDataV2() {
	}

	/**
	 * Instantiates a new variant run data.
	 *
	 * @param id the id
	 */
	public VariantRunDataV2(VariantRunDataId id) {
		this.id = id;
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public VariantRunDataId getId() {
		return id;
	}

	/**
	 * Sets the id.
	 *
	 * @param id the new id
	 */
	public void setId(VariantRunDataId id) {
		this.id = id;
	}

	/**
	 * Gets the run name.
	 *
	 * @return the run name
	 */
	public String getRunName() {
		return getId().getRunName();
	}

	/**
	 * Gets the sample genotypes.
	 *
	 * @return the sample genotypes
	 */
	public HashMap<Integer, SampleGenotype> getSampleGenotypes() {
		return sampleGenotypes;
	}

	/**
	 * Sets the sample genotypes.
	 *
	 * @param genotypes the genotypes
	 */
	public void setSampleGenotypes(HashMap<Integer, SampleGenotype> genotypes) {
		this.sampleGenotypes = genotypes;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
    @Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof VariantRunData))
			return false;
		
		return getId().equals(((VariantRunData)o).getId());
	}
    
	@Override
	public int hashCode()	// thanks to this overriding, HashSet.contains will find such objects based on their ID
	{
		if (getId() == null)
			return super.hashCode();

		return getId().hashCode();
	}
	
	@Override
	public String toString()
	{
		if (getId() == null)
			return super.toString();

		return getId().toString();
	}
}
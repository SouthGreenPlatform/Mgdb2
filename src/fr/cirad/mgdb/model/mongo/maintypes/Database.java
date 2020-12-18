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

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class Database.
 */
@Document(collection = "databases")
@TypeAlias("D")
public class Database/* implements Comparable<Database>*/
{
	public final static String FIELDNAME_HOSTCLIENT = "c";
	
	public final static String FIELDNAME_NAME = "n";

	public final static String FIELDNAME_TAXID = "i";
	
	public final static String FIELDNAME_TAXON = "t";
	
	public final static String FIELDNAME_SPECIES = "s";
	
	public final static String FIELDNAME_PUBLIC = "p";

	public final static String FIELDNAME_HIDDEN = "h";
	
	/** The id. */
	@Id
	private String id;

	/** The host client name. */
	@Field(FIELDNAME_HOSTCLIENT)
	private String host;

	/** The db name. */
	@Field(FIELDNAME_NAME)
	private String name;
	
	/** The taxon id. */
	@Field(FIELDNAME_TAXID)
	private Integer taxid;
	
	/** The taxon name. */
	@Field(FIELDNAME_TAXON)
	private String taxon;

	/** The species name. */
	@Field(FIELDNAME_SPECIES)
	private String species;
	
	/** The public flag. */
	@Field(FIELDNAME_PUBLIC)
	private boolean fPublic;

	/** The hidden flag. */
	@Field(FIELDNAME_HIDDEN)
	private boolean hidden;
	
	/**
	 * Instantiates a new database.
	 *
	 * @param id the id
	 */
	public Database(String id) {
		this.id = id;
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getTaxid() {
		return taxid;
	}

	public void setTaxid(Integer taxid) {
		this.taxid = taxid;
	}

	public String getTaxon() {
		return taxon;
	}

	public void setTaxon(String taxon) {
		this.taxon = taxon;
	}

	public String getSpecies() {
		return species;
	}

	public void setSpecies(String species) {
		this.species = species;
	}

	public boolean isPublic() {
		return fPublic;
	}

	public void setPublic(boolean fPublic) {
		this.fPublic = fPublic;
	}

	public boolean isHidden() {
		return hidden;
	}

	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof Database))
			return false;
		
		return getId().equals(((Database)o).getId());
	}
	
//	@Override
//	public int compareTo(Database other)
//	{
//		return getId().compareToIgnoreCase(other.getId());
//	}
	
	@Override
	public String toString() {
		return id;
	}
}

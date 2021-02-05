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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.tools.Helper;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

abstract public class AbstractVariantData {
	/** The Constant FIELDNAME_ANALYSIS_METHODS. */
	public final static String FIELDNAME_ANALYSIS_METHODS = "m";
	
	/** The Constant FIELDNAME_SYNONYMS. */
	public final static String FIELDNAME_SYNONYMS = "s";
	
	/** The Constant FIELDNAME_KNOWN_ALLELE_LIST. */
	public final static String FIELDNAME_KNOWN_ALLELE_LIST = "a";
	
	/** The Constant FIELDNAME_TYPE. */
	public final static String FIELDNAME_TYPE = "t";
	
	/** The Constant FIELDNAME_REFERENCE_POSITION. */
	public final static String FIELDNAME_REFERENCE_POSITION = "p";
	
//	/** The Constant FIELDNAME_PROJECT_DATA. */
//	public final static String FIELDNAME_PROJECT_DATA = "pj";
	
	/** The Constant FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA. */
	public final static String FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA = "i";
	
	/** The Constant FIELDNAME_SYNONYM_TYPE_ID_NCBI. */
	public final static String FIELDNAME_SYNONYM_TYPE_ID_NCBI = "r";
	
	/** The Constant FIELDNAME_SYNONYM_TYPE_ID_INTERNAL. */
	public final static String FIELDNAME_SYNONYM_TYPE_ID_INTERNAL = "n";
	
	/** The Constant SECTION_ADDITIONAL_INFO. */
	public final static String SECTION_ADDITIONAL_INFO = "i";

	/** The Constant FIELD_PHREDSCALEDQUAL, expected to be found in VCF files */
	public static final String FIELD_PHREDSCALEDQUAL = "qual";
	
	/** The Constant FIELD_SOURCE, expected to be found in VCF files */
	public static final String FIELD_SOURCE = "name";
	
	/** The Constant FIELD_FILTERS, expected to be found in VCF files */
	public static final String FIELD_FILTERS = "filt";
	
	/** The Constant FIELD_FULLYDECODED, expected to be found in VCF files */
	public static final String FIELD_FULLYDECODED = "fullDecod";
	
	/** The Constant FIELDVAL_SOURCE_MISSING, expected to be found in VCF files. */
	public static final String FIELDVAL_SOURCE_MISSING = "Unknown";
	
	/** The Constant GT_FIELD_GQ, expected to be found in VCF files */
	public static final String GT_FIELD_GQ = "GQ";
	
	/** The Constant GT_FIELD_DP, expected to be found in VCF files */
	public static final String GT_FIELD_DP = "DP";
	
	/** The Constant GT_FIELD_AD, expected to be found in VCF files */
	public static final String GT_FIELD_AD = "AD";
	
	/** The Constant GT_FIELD_PL, expected to be found in VCF files */
	public static final String GT_FIELD_PL = "PL";
	
	/** The Constant GT_FIELD_PHASED_GT. */
	public static final String GT_FIELD_PHASED_GT = "phGT";
	
	/** The Constant GT_FIELD_PHASED_ID. */
	public static final String GT_FIELD_PHASED_ID = "phID";

	/** The Constant GT_FIELDVAL_AL_MISSING. */
	public static final String GT_FIELDVAL_AL_MISSING = ".";
	
	/** The Constant GT_FIELDVAL_ID_MISSING. */
	public static final String GT_FIELDVAL_ID_MISSING = ".";
	
	/** The id. */
	protected Object id;
	
	/** The type. */
	@Field(FIELDNAME_TYPE)
	private String type;

	/** The reference position. */
	@Field(FIELDNAME_REFERENCE_POSITION)
	protected Map<Integer, ReferencePosition> referencePositions = new HashMap<>();
	
	/** The synonyms. */
	@Field(FIELDNAME_SYNONYMS)
	private TreeMap<String /*synonym type*/, TreeSet<String>> synonyms;

	/** The analysis methods. */
	@Field(FIELDNAME_ANALYSIS_METHODS)
	private TreeSet<String> analysisMethods = null;

	/** The known allele list. */
	@Field(FIELDNAME_KNOWN_ALLELE_LIST)
	protected List<String> knownAlleleList;

	/** The additional info. */
	@Field(SECTION_ADDITIONAL_INFO)
	private HashMap<String, Object> additionalInfo = null;
	
	protected static Document projectionDoc(int nAssemblyId) {
		return new Document(VariantData.FIELDNAME_REFERENCE_POSITION + "." + nAssemblyId + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(VariantData.FIELDNAME_REFERENCE_POSITION + "." + nAssemblyId + "." + ReferencePosition.FIELDNAME_START_SITE, 1);	
	}

	/**
	 * Fixes AD array in the case where provided alleles are different from the order in which we have them in the DB
	 * @param importedAD
	 * @param importedAlleles
	 * @param knownAlleles
	 * @return
	 */
	static public int[] fixAdFieldValue(int[] importedAD, List<? extends Comparable> importedAlleles, List<String> knownAlleles)
    {
    	List<String> importedAllelesAsStrings = importedAlleles.stream().filter(allele -> Allele.class.isAssignableFrom(allele.getClass()))
    				.map(Allele.class::cast)
    				.map(allele -> allele.getBaseString()).collect(Collectors.toList());
    	
    	if (importedAllelesAsStrings.isEmpty())
    		importedAllelesAsStrings.addAll((Collection<? extends String>) importedAlleles);
    	
    	if (Arrays.equals(knownAlleles.toArray(), importedAllelesAsStrings.toArray()))
    	{
//    		System.out.println("AD: no fix needed for " + Helper.arrayToCsv(", ", importedAD));
    		return importedAD;
    	}

    	HashMap<Integer, Integer> knownAlleleToImportedAlleleIndexMap = new HashMap<>();
    	for (int i=0; i<importedAlleles.size(); i++)
    	{
    		String allele = importedAllelesAsStrings.get(i);
    		int knownAlleleIndex = knownAlleles.indexOf(allele);
    		if (knownAlleleToImportedAlleleIndexMap.get(knownAlleleIndex) == null)
    			knownAlleleToImportedAlleleIndexMap.put(knownAlleleIndex, i);
    	}
    	int[] adToStore = new int[knownAlleles.size()];
    	for (int i=0; i<adToStore.length; i++)
    	{
    		Integer importedAlleleIndex = knownAlleleToImportedAlleleIndexMap.get(i);
    		adToStore[i] = importedAlleleIndex == null ? 0 : importedAD[importedAlleleIndex];
    	}
//		System.out.println("AD: " + Helper.arrayToCsv(", ", importedAD) + " -> " + Helper.arrayToCsv(", ", adToStore));

    	return adToStore;
    }

	static public int[] fixPlFieldValue(int[] importedPL, int ploidy, List<? extends Comparable> importedAlleles, List<String> knownAlleles)
	{
    	List<String> importedAllelesAsStrings = importedAlleles.stream().filter(allele -> Allele.class.isAssignableFrom(allele.getClass()))
				.map(Allele.class::cast)
				.map(allele -> allele.getBaseString()).collect(Collectors.toList());
    	
    	if (importedAllelesAsStrings.isEmpty())
    		importedAllelesAsStrings.addAll((Collection<? extends String>) importedAlleles);
	
    	if (Arrays.equals(knownAlleles.toArray(), importedAllelesAsStrings.toArray()))
    	{
//    		System.out.println("PL: no fix needed for " + Helper.arrayToCsv(", ", importedPL));
    		return importedPL;
    	}
    	
    	HashMap<Integer, Integer> knownAlleleToImportedAlleleIndexMap = new HashMap<>();
    	for (int i=0; i<importedAlleles.size(); i++)
    	{
    		String allele = importedAllelesAsStrings.get(i);
    		int knownAlleleIndex = knownAlleles.indexOf(allele);
    		if (knownAlleleToImportedAlleleIndexMap.get(knownAlleleIndex) == null)
    			knownAlleleToImportedAlleleIndexMap.put(knownAlleleIndex, i);
    	}
    	
    	int[] plToStore = new int[bcf_ap2g(knownAlleles.size(), ploidy)];
    	for (int i=0; i<plToStore.length; i++)
    	{
    		int[] genotype = bcf_ip2g(i, ploidy);
    		for (int j=0; j<genotype.length; j++)	// convert genotype to match the provided allele ordering
    		{
    			Integer importedAllele = knownAlleleToImportedAlleleIndexMap.get(genotype[j]);
    			if (importedAllele == null)
    			{
    				genotype = null;
    				break;	// if any allele is not part of the imported ones then the whole genotype is not represented
    			}
    			else
    				genotype[j] = importedAllele;
    		}
    		if (genotype != null)
    			Arrays.sort(genotype);
    		
    		plToStore[i] = genotype == null ? Integer.MAX_VALUE : importedPL[(int) bcf_g2i(genotype, ploidy)];
    	}
//    	System.out.println("PL: " + Helper.arrayToCsv(", ", importedPL) + " -> " + Helper.arrayToCsv(", ", plToStore));
    	
		return plToStore;
	}
	
	/**
	 * Gets number of genotypes from number of alleles and ploidy.
	 * Translated from original C++ code that was part of the project https://github.com/atks/vt
	 */
	static public int bcf_ap2g(int no_allele, int no_ploidy)
	{
		if (no_ploidy==1 || no_allele<=1)
	        return no_allele;
	    else if (no_ploidy==2)
	        return (((no_allele+1)*(no_allele))>>1);
	    else
	        return (int) Helper.choose(no_ploidy+no_allele-1, no_allele-1);
	}
		
	/**
	 * Gets index of a genotype of n ploidy.
	 * Translated from original C++ code that was part of the project https://github.com/atks/vt
	 */
	static public int bcf_g2i(int[] g, int n)
	{
	    if (n==1)
	        return g[0];
	    if (n==2)
	        return g[0] + (((g[1]+1)*(g[1]))>>1);
	    else
	    {
	    	int index = 0;
	        for (int i=0; i<n; ++i)
	            index += bcf_ap2g(g[i], i+1);
	        return index;
	    }
	}
	
	/**
	 * Gets genotype from genotype index and ploidy.
	 * Translated from original C++ code that was part of the project https://github.com/atks/vt
	 */
	static public int[] bcf_ip2g(int genotype_index, int no_ploidy)
	{
	    int[] genotype = new int[no_ploidy];
	    int pth = no_ploidy;
	    int max_allele_index = genotype_index;
	    int leftover_genotype_index = genotype_index;
	    while (pth>0)
	    {
	        for (int allele_index=0; allele_index <= max_allele_index; ++allele_index)
	        {
	            double i = Helper.choose(pth+allele_index-1, pth);
	            if (i>=leftover_genotype_index || allele_index==max_allele_index)
	            {
	                if (i>leftover_genotype_index)
	                	--allele_index;
	                leftover_genotype_index -= Helper.choose(pth+allele_index-1, pth);
	                --pth;
	                max_allele_index = allele_index;
	                genotype[pth] = allele_index;
	                break;                
	            }
	        }
	    }
	    return genotype;
	}
  
//  static public List<Integer> getAlleles(final int PLindex, final int ploidy) {
//      if ( ploidy == 2 ) { // diploid
//          final GenotypeLikelihoodsAllelePair pair = getAllelePair(PLindex);
//          return Arrays.asList(pair.alleleIndex1, pair.alleleIndex2);
//      } else { // non-diploid
//          if (!anyploidPloidyToPLIndexToAlleleIndices.containsKey(ploidy))
//              throw new IllegalStateException("Must initialize the cache of allele anyploid indices for ploidy " + ploidy);
//
//          if (PLindex < 0 || PLindex >= anyploidPloidyToPLIndexToAlleleIndices.get(ploidy).size()) {
//              final String msg = "The PL index " + PLindex + " does not exist for " + ploidy + " ploidy, " +
//                      (PLindex < 0 ? "cannot have a negative value." : "initialized the cache of allele anyploid indices with the incorrect number of alternate alleles.");
//              throw new IllegalStateException(msg);
//          }
//
//          return anyploidPloidyToPLIndexToAlleleIndices.get(ploidy).get(PLindex);
//      }
//}
	
//	static public int likelihoodGtIndex(int j, int k)
//    {
//    	return (k*(k+1)/2)+j;
//    }
//    
//	/**
//	 * Instantiates a new variant data.
//	 */
//	public VariantData() {
//		super();
//	}
//	
//	/**
//	 * Instantiates a new variant data.
//	 *
//	 * @param id the id
//	 */
//	public VariantData(Comparable id) {
//		super();
//		this.id = id;
//	}
//	
//	/**
//	 * Gets the id.
//	 *
//	 * @return the id
//	 */
//	public Comparable getId() {
//		return id;
//	}

	/**
	 * Gets the synonyms.
	 *
	 * @return the synonyms
	 */
	public TreeMap<String, TreeSet<String>> getSynonyms() {
		if (synonyms == null)
			synonyms = new TreeMap<>();
		return synonyms;
	}

	/**
	 * Sets the synonyms.
	 *
	 * @param synonyms the synonyms
	 */
	public void setSynonyms(TreeMap<String, TreeSet<String>> synonyms) {
		this.synonyms = synonyms;
	}
	
	public TreeSet<String> getAnalysisMethods() {
		return analysisMethods;
	}

	public void setAnalysisMethods(TreeSet<String> analysisMethods) {
		this.analysisMethods = analysisMethods;
	}

	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * Sets the type.
	 *
	 * @param type the new type
	 */
	public void setType(String type) {
		this.type = type.intern();
	}

	/**
	 * Gets the reference positions by assembly ID.
	 *
	 * @return the reference positions
	 */
	public Map<Integer, ReferencePosition> getReferencePositions() {
		return referencePositions;
	}
	
	/**
	 * Gets the reference position for a given assembly ID.
	 *
	 * @return the reference position
	 */
	public ReferencePosition getReferencePosition(int nAssemblyId) {
		return referencePositions.get(nAssemblyId);
	}

	/**
	 * Sets the reference positions.
	 *
	 * @param referencePositions the new reference position
	 */
	public void setReferencePositions(Map<Integer, ReferencePosition> referencePositions) {
		this.referencePositions = referencePositions;
	}
	

	/**
	 * Sets the reference position for a given assembly ID.
	 *
	 * @param referencePosition the new reference position
	 */
	public void setReferencePosition(int nAssemblyId, ReferencePosition referencePosition) {
		referencePositions.put(nAssemblyId, referencePosition);
	}
	
	/**
	 * Gets the known allele list.
	 *
	 * @return the known allele list
	 */
	public List<String> getKnownAlleleList() {
		if (knownAlleleList == null)
			knownAlleleList = new ArrayList<String>();
		return knownAlleleList;
	}

	/**
	 * Sets the known allele list.
	 *
	 * @param knownAlleleList the new known allele list
	 */
	public void setKnownAlleleList(List<String> knownAlleleList) {
		this.knownAlleleList = knownAlleleList;
		for (String allele : this.knownAlleleList)
			allele.intern();
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
	
	/**
	 * Static get alleles from genotype code.
	 *
	 * @param alleleList the allele list
	 * @param code the code
	 * @return the list
	 * @throws Exception the exception
	 */
	static public List<String> staticGetAllelesFromGenotypeCode(List<String> alleleList, String code) throws Exception
	{
		ArrayList<String> result = new ArrayList<String>();
		if (code != null)
		{
			for (String alleleCodeIndex : Helper.split(code.replaceAll("\\|", "/"), "/"))
			{
				int nAlleleCodeIndex = Integer.parseInt(alleleCodeIndex);
				if (alleleList.size() > nAlleleCodeIndex)
					result.add(alleleList.get(nAlleleCodeIndex));
				else
					throw new Exception("Variant has no allele number " + nAlleleCodeIndex);
			}
		}
		return result;
	}

	/**
	 * Gets the alleles from genotype code.
	 *
	 * @param code the code
	 * @return the alleles from genotype code
	 * @throws Exception the exception
	 */
	public List<String> getAllelesFromGenotypeCode(String code) throws Exception
	{
		try
		{
			return staticGetAllelesFromGenotypeCode(knownAlleleList, code);
		}
		catch (Exception e)
		{
			throw new Exception("Variant ID: " + id + " - " + e.getMessage());
		}
	}
	
	/**
	 * Rebuild vcf format genotype.
	 *
	 * @param alternates the alternates
	 * @param genotypeAlleles the genotype alleles
	 * @param fPhased whether or not the genotype is phased
	 * @param keepCurrentPhasingInfo the keep current phasing info
	 * @return the string
	 * @throws Exception the exception
	 */
	public static String rebuildVcfFormatGenotype(List<String> knownAlleleList, List<String> genotypeAlleles, boolean fPhased, boolean keepCurrentPhasingInfo) throws Exception
	{
		String result = "";
		List<String> orderedGenotypeAlleles = new ArrayList<String>();
		orderedGenotypeAlleles.addAll(genotypeAlleles);
		mainLoop: for (String gtA : orderedGenotypeAlleles)
		{
			String separator = keepCurrentPhasingInfo && fPhased ? "|" : "/";
			for (int i=0; i<knownAlleleList.size(); i++)
			{
				String allele = knownAlleleList.get(i);
				if (allele.equals(gtA))
				{
					result += (result.length() == 0 ? "" : separator) + i;
					continue mainLoop;						
				}
			}
			if (!GT_FIELDVAL_AL_MISSING.equals(gtA))
				throw new Exception("Unable to find allele '" + gtA + "' in alternate list");
		}

		return result.length() > 0 ? result : null;
	}

	// tells whether applied filters imply to treat this genotype as missing data
    public static boolean gtPassesVcfAnnotationFilters(String individualName, int sampleId, HashMap<String, HashMap<Integer, Object>> metadata, Collection<String> individuals1, HashMap<String, Float> annotationFieldThresholds, Collection<String> individuals2, HashMap<String, Float> annotationFieldThresholds2)
    {
		List<HashMap<String, Float>> thresholdsToCheck = new ArrayList<HashMap<String, Float>>();
		if (individuals1.contains(individualName))
			thresholdsToCheck.add(annotationFieldThresholds);
		if (individuals2.contains(individualName))
			thresholdsToCheck.add(annotationFieldThresholds2);
		
		for (HashMap<String, Float> someThresholdsToCheck : thresholdsToCheck)
			for (String annotationField : someThresholdsToCheck.keySet())
			{
				Integer annotationValue = null;
				try
				{
					annotationValue = (Integer) metadata.get(annotationField).get(sampleId);
				}
				catch (Exception ignored)
				{}
				if (annotationValue != null && annotationValue < someThresholdsToCheck.get(annotationField))
					return false;
			}
		return true;
	}
    
	// tells whether applied filters imply to treat this genotype as missing data
    public static boolean gtPassesVcfAnnotationFiltersV2(String individualName, SampleGenotype sampleGenotype, Collection<String> individuals1, HashMap<String, Float> annotationFieldThresholds, Collection<String> individuals2, HashMap<String, Float> annotationFieldThresholds2)
    {
		List<HashMap<String, Float>> thresholdsToCheck = new ArrayList<HashMap<String, Float>>();
		if (individuals1.contains(individualName))
			thresholdsToCheck.add(annotationFieldThresholds);
		if (individuals2.contains(individualName))
			thresholdsToCheck.add(annotationFieldThresholds2);
		
		for (HashMap<String, Float> someThresholdsToCheck : thresholdsToCheck)
			for (String annotationField : someThresholdsToCheck.keySet())
			{
				Integer annotationValue = null;
				try
				{
					annotationValue = (Integer) sampleGenotype.getAdditionalInfo().get(annotationField);
				}
				catch (Exception ignored)
				{}
				if (annotationValue != null && annotationValue < someThresholdsToCheck.get(annotationField))
					return false;
			}
		return true;
	}
    
    /* based on code from htsjdk.variant.variantcontext.VariantContext v2.14.3 */
    public static Type determinePolymorphicType(List<String> alleles) {
    	
        switch ( alleles.size() ) {
	        case 0:
	            throw new IllegalStateException("Unexpected error: requested type of Variant with no alleles!");
	        case 1:
	            // note that this doesn't require a reference allele.  You can be monomorphic independent of having a
	            // reference allele
	            return Type.NO_VARIATION;
	        default: {
	        	Type type = null;

	            // do a pairwise comparison of all alleles against the reference allele
	            for ( String allele : alleles ) {
	                if ( allele == alleles.get(0) )
	                    continue;

	                // find the type of this allele relative to the reference
	                Type biallelicType = typeOfBiallelicVariant(alleles.get(0), allele);

	                // for the first alternate allele, set the type to be that one
	                if ( type == null ) {
	                    type = biallelicType;
	                }
	                // if the type of this allele is different from that of a previous one, assign it the MIXED type and quit
	                else if ( biallelicType != type ) {
	                    type = Type.MIXED;
	                    break;
	                }
	            }
	            return type;
	        }
	    }

    }

    /* based on code from htsjdk.variant.variantcontext.VariantContext v2.14.3 */
    private static Type typeOfBiallelicVariant(String ref, String allele) {
        if ( "*".equals(ref) )
            throw new IllegalStateException("Unexpected error: encountered a record with a symbolic reference allele");

        if ( "*".equals(allele) )
            return Type.SYMBOLIC;

        if ( ref.length() == allele.length() ) {
            if ( allele.length() == 1 )
                return Type.SNP;
            else
                return Type.MNP;
        }

        // Important note: previously we were checking that one allele is the prefix of the other.  However, that's not an
        // appropriate check as can be seen from the following example:
        // REF = CTTA and ALT = C,CT,CA
        // This should be assigned the INDEL type but was being marked as a MIXED type because of the prefix check.
        // In truth, it should be absolutely impossible to return a MIXED type from this method because it simply
        // performs a pairwise comparison of a single alternate allele against the reference allele (whereas the MIXED type
        // is reserved for cases of multiple alternate alleles of different types).  Therefore, if we've reached this point
        // in the code (so we're not a SNP, MNP, or symbolic allele), we absolutely must be an INDEL.

        return Type.INDEL;

        // old incorrect logic:
        // if (oneIsPrefixOfOther(ref, allele))
        //     return Type.INDEL;
        // else
        //     return Type.MIXED;
    }
}
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

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.tools.Helper;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;

/**
 * The Class VariantData.
 */
@Document(collection = "variants")
@TypeAlias("VD")
//@CompoundIndexes({
//    @CompoundIndex(def = "{'" + VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE + "': 1, '" + VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE + "': 1}")
//})
public class VariantData extends AbstractVariantData
{
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);
	
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
	public VariantData() {
		super();
	}
	
	/**
	 * Instantiates a new variant data.
	 *
	 * @param id the id
	 */
	public VariantData(String id) {
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
		
		if (o == null || !(o instanceof VariantData))
			return false;
		
		return getId().equals(((VariantData)o).getId());
	}
	
	@Override
	public int hashCode()	// thanks to this overriding, HashSet.contains will find such objects based on their ID
	{
		if (getId() == null)
			return super.hashCode();

		return getId().hashCode();
	}
	
	
/**
 * To variant context.
 *
 * @param runs the runs
 * @param nAssemblyId ID of the assembly to work with
 * @param exportVariantIDs the export variant ids
 * @param samplesToExport list of samples to include data for
 * @param individuals1 individual IDs for group 1
 * @param individuals2 individual IDs for group 2
 * @param previousPhasingIds the previous phasing ids
 * @param annotationFieldThresholds1 the annotation field thresholds for group 1
 * @param annotationFieldThresholds2 the annotation field thresholds for group 2
 * @param warningFileWriter the warning file writer
 * @param synonym the synonym
 * @return the variant context
 * @throws Exception the exception
 */
public VariantContext toVariantContext(Collection<VariantRunData> runs, int nAssemblyId, boolean exportVariantIDs, Collection<GenotypingSample> samplesToExport, Collection<String> individuals1, Collection<String> individuals2, HashMap<Integer, Object> previousPhasingIds, HashMap<String, Float> annotationFieldThresholds1, HashMap<String, Float> annotationFieldThresholds2, FileWriter warningFileWriter, Comparable synonym) throws Exception
{
	ArrayList<Genotype> genotypes = new ArrayList<Genotype>();
	String sRefAllele = knownAlleleList.size() == 0 ? "" : knownAlleleList.get(0);

	ArrayList<Allele> variantAlleles = new ArrayList<Allele>();
	variantAlleles.add(Allele.create(sRefAllele, true));
	
	// collect all genotypes for all individuals
	Map<String/*individual*/, HashMap<String/*genotype code*/, List<GenotypingSample>>> individualSamplesByGenotype = new LinkedHashMap<>();
	
//	HashMap<Integer, String> sampleGenotypes = new HashMap<>();
	List<VariantRunData> runsWhereDataWasFound = new ArrayList<>();
	List<String> individualList = new ArrayList<>();
	for (GenotypingSample sample : samplesToExport)
	{
		if (runs == null || runs.size() == 0)
			continue;
		
		Integer sampleIndex = sample.getId();
		
		for (VariantRunData run : runs)
		{
			String gtCode = run.getGenotypes().get(sampleIndex);
			if (gtCode == null)
				continue;	// run contains no data for this sample
			
			// keep track of runs so we can have access to additional info later on
//			sampleGenotypes.put(sampleIndex, gtCode);
			if (!runsWhereDataWasFound.contains(run))
				runsWhereDataWasFound.add(run);
			
			String individualId = sample.getIndividual();
			if (!individualList.contains(individualId))
				individualList.add(individualId);
			HashMap<String, List<GenotypingSample>> storedIndividualGenotypes = individualSamplesByGenotype.get(individualId);
			if (storedIndividualGenotypes == null) {
				storedIndividualGenotypes = new HashMap<>();
				individualSamplesByGenotype.put(individualId, storedIndividualGenotypes);
			}
			List<GenotypingSample> samplesWithGivenGenotype = storedIndividualGenotypes.get(gtCode);
			if (samplesWithGivenGenotype == null)
			{
				samplesWithGivenGenotype = new ArrayList<>();
				storedIndividualGenotypes.put(gtCode, samplesWithGivenGenotype);
			}
			samplesWithGivenGenotype.add(sample);
		}
	}
	
	if (runsWhereDataWasFound.size() == 0)
		LOG.info("No run data found for variant " + getId());
	VariantRunData run = runsWhereDataWasFound.size() == 1 ? runsWhereDataWasFound.get(0) : null;	// if there is not exactly one run involved then we do not export metadata

	if (run != null) {
		HashMap<GenotypingSample, GenotypeBuilder> gtBuilders = new HashMap<>();
		int nPloidy = 0;
		for (String individualName : individualList) {
			HashMap<String, List<GenotypingSample>> samplesWithGivenGenotype = individualSamplesByGenotype.get(individualName);
			HashMap<Object, Integer> genotypeCounts = new HashMap<Object, Integer>(); // will help us to keep track of missing genotypes
				
			int highestGenotypeCount = 0;
			String mostFrequentGenotype = null;
			if (genotypes != null && samplesWithGivenGenotype != null)
				for (String gtCode : samplesWithGivenGenotype.keySet()) {
					if (gtCode == null)
						continue; /* skip missing genotypes */

					int gtCount = samplesWithGivenGenotype.get(gtCode).size();
					if (gtCount > highestGenotypeCount) {
						highestGenotypeCount = gtCount;
						mostFrequentGenotype = gtCode;
					}
					genotypeCounts.put(gtCode, gtCount);
				}
			
			if (mostFrequentGenotype == null)
				continue;	// no genotype for this individual
			
			if (warningFileWriter != null && genotypeCounts.size() > 1)
				warningFileWriter.write("- Dissimilar genotypes found for variant " + (synonym == null ? id : synonym) + ", individual " + individualName + ". Exporting most frequent: " + mostFrequentGenotype + "\n");
			
			GenotypingSample sample = samplesWithGivenGenotype.get(mostFrequentGenotype).get(0);	// any will do
			
			Object currentPhId = run.getMetadata().get(GT_FIELD_PHASED_ID).get(sample.getId());
			
			boolean isPhased = currentPhId != null && currentPhId.equals(previousPhasingIds.get(sample.getId()));

			List<String> alleles = getAllelesFromGenotypeCode(isPhased ? (String) run.getMetadata().get(GT_FIELD_PHASED_GT).get(sample.getId()) : mostFrequentGenotype);
			ArrayList<Allele> individualAlleles = new ArrayList<Allele>();
			previousPhasingIds.put(sample.getId(), currentPhId == null ? id : currentPhId);
			if (alleles.size() == 0)
				continue;	/* skip this sample because there is no genotype for it */
			
			boolean fAllAllelesNoCall = true;
			for (String allele : alleles)
				if (allele.length() > 0) {
					fAllAllelesNoCall = false;
					break;
				}
			for (String sAllele : alleles) {
				Allele allele = Allele.create(sAllele.length() == 0 ? (fAllAllelesNoCall ? Allele.NO_CALL_STRING : "<DEL>") : sAllele, sRefAllele.equals(sAllele));
				if (!allele.isNoCall() && !variantAlleles.contains(allele))
					variantAlleles.add(allele);
				individualAlleles.add(allele);
			}

			GenotypeBuilder gb = new GenotypeBuilder(individualName, individualAlleles);
			if (individualAlleles.size() > 0) {
				if (nPloidy == 0)
					nPloidy = individualAlleles.size();
				gb.phased(isPhased);
				String genotypeFilters = (String) run.getMetadata().get(sample.getId()).get(FIELD_FILTERS);
				if (genotypeFilters != null && genotypeFilters.length() > 0)
					gb.filter(genotypeFilters);
			}
			gtBuilders.put(sample, gb);
		}
		
		if (nPloidy == 0)
			LOG.info("No alleles found for any sample in variant " + getId());
		else 
			individualLoop : for (GenotypingSample sample : gtBuilders.keySet()) {
				GenotypeBuilder gb = gtBuilders.get(sample.getIndividual());
				List<String> alleleListAtImportTimeIfDifferentFromNow = null;
				for (String key : run.getMetadata().keySet())
				{
					HashMap<Integer, Object> perSampleFieldData = run.getMetadata().get(key);
					if (!gtPassesVcfAnnotationFilters(sample.getIndividual(), sample.getId(), run.getMetadata(), individuals1, annotationFieldThresholds1, individuals2, annotationFieldThresholds2))
						continue individualLoop;	// skip genotype

					if (VCFConstants.GENOTYPE_ALLELE_DEPTHS.equals(key))
					{
						String ad = (String) run.getMetadata().get(key).get(sample.getId());
						if (ad != null)
						{
							int[] adArray = Helper.csvToIntArray(ad);
							if (knownAlleleList.size() > adArray.length)
							{
								alleleListAtImportTimeIfDifferentFromNow = knownAlleleList.subList(0, adArray.length);
								adArray = VariantData.fixAdFieldValue(adArray, alleleListAtImportTimeIfDifferentFromNow, knownAlleleList);
							}
							gb.AD(adArray);
						}
					}
					else if (VCFConstants.DEPTH_KEY.equals(key) || VCFConstants.GENOTYPE_QUALITY_KEY.equals(key))
					{
						Integer value = (Integer) run.getMetadata().get(key).get(sample.getId());
						if (value != null)
						{
							if (VCFConstants.DEPTH_KEY.equals(key))
								gb.DP(value);
							else
								gb.GQ(value);
						}
					}
					else if (VCFConstants.GENOTYPE_PL_KEY.equals(key) || VCFConstants.GENOTYPE_LIKELIHOODS_KEY.equals(key))
					{
						String fieldVal = (String) run.getMetadata().get(key).get(sample.getId());
						if (fieldVal != null)
						{
							int[] plArray = VCFConstants.GENOTYPE_PL_KEY.equals(key) ? Helper.csvToIntArray(fieldVal) : GenotypeLikelihoods.fromGLField(fieldVal).getAsPLs();
							if (alleleListAtImportTimeIfDifferentFromNow != null)
								plArray = VariantData.fixPlFieldValue(plArray, nPloidy, alleleListAtImportTimeIfDifferentFromNow, knownAlleleList);
							gb.PL(plArray);
						}
					}
					else if (!key.equals(VariantData.GT_FIELD_PHASED_GT) && !key.equals(VariantData.GT_FIELD_PHASED_ID) && !key.equals(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE) && !key.equals(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME)) // exclude some internally created fields that we don't want to export
						gb.attribute(key, run.getMetadata().get(key).get(sample.getId())); // looks like we have an extended attribute				
				}
				genotypes.add(gb.make());
			}
	}

	String source = run == null ? null : (String) run.getAdditionalInfo().get(FIELD_SOURCE);

	ReferencePosition referencePosition = referencePositions.get(nAssemblyId);
	Long start = referencePosition == null ? null : referencePosition.getStartSite(), stop = referencePosition == null ? null : (referencePosition.getEndSite() == null ? start : referencePosition.getEndSite());
	String chr = referencePosition == null ? null : referencePosition.getSequence();
	VariantContextBuilder vcb = new VariantContextBuilder(source != null ? source : FIELDVAL_SOURCE_MISSING, chr != null ? chr : "", start != null ? start : 0, stop != null ? stop : 0, variantAlleles);
	if (exportVariantIDs)
		vcb.id((synonym == null ? id : synonym).toString());
	vcb.genotypes(genotypes);
	
	if (run != null) {
		Boolean fullDecod = (Boolean) run.getAdditionalInfo().get(FIELD_FULLYDECODED);
		vcb.fullyDecoded(fullDecod != null && fullDecod);

		String filters = (String) run.getAdditionalInfo().get(FIELD_FILTERS);
		if (filters != null)
			vcb.filters(filters.split(","));
		else
			vcb.filters(VCFConstants.UNFILTERED);
		
		Number qual = (Number) run.getAdditionalInfo().get(FIELD_PHREDSCALEDQUAL);
		if (qual != null)
			vcb.log10PError(qual.doubleValue() / -10.0D);
		
		List<String> alreadyTreatedAdditionalInfoFields = Arrays.asList(new String[] {FIELD_SOURCE, FIELD_FULLYDECODED, FIELD_FILTERS, FIELD_PHREDSCALEDQUAL});
		for (String attrName : run.getAdditionalInfo().keySet())
			if (!VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME.equals(attrName) && !VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE.equals(attrName) && !alreadyTreatedAdditionalInfoFields.contains(attrName))
				vcb.attribute(attrName, run.getAdditionalInfo().get(attrName));
	}
	VariantContext vc = vcb.make();
	return vc;
}
}
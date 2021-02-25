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
package fr.cirad.mgdb.importing;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapCodec;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapFeature;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.variantcontext.VariantContext.Type;

/**
 * The Class HapMapImport.
 */
public class HapMapImport extends AbstractGenotypeImport {

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);
	
    public boolean m_fCloseContextOpenAfterImport = false;

	/** The m_process id. */
	private String m_processID;

	private static HashMap<String, String> iupacCodeConversionMap = new HashMap<>();

	static
	{
		iupacCodeConversionMap.put("A", "AA");
		iupacCodeConversionMap.put("C", "CC");
		iupacCodeConversionMap.put("G", "GG");
		iupacCodeConversionMap.put("T", "TT");
		iupacCodeConversionMap.put("U", "TT");
		iupacCodeConversionMap.put("R", "AG");
		iupacCodeConversionMap.put("Y", "CT");
		iupacCodeConversionMap.put("S", "GC");
		iupacCodeConversionMap.put("W", "AT");
		iupacCodeConversionMap.put("K", "GT");
		iupacCodeConversionMap.put("M", "AC");
		iupacCodeConversionMap.put("N", "NN");
	}
	
	/**
	 * Instantiates a new hapmap import.
	 */
	public HapMapImport()
	{
	}

	/**
	 * Instantiates a new hapmap import.
	 *
	 * @param processID the process id
	 */
	public HapMapImport(String processID)
	{
		m_processID = processID;
	}
	
	/**
     * Instantiates a new hapmap import.
     */
    public HapMapImport(boolean fCloseContextOpenAfterImport) {
        this();
    	m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }
    
    /**
     * Instantiates a new hapmap import.
     */
    public HapMapImport(boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this();
    	m_fCloseContextAfterImport = fCloseContextAfterImport;
    	m_fAllowNewAssembly = fAllowNewAssembly;
    }

    /**
     * Instantiates a new hapmap import.
     */
    public HapMapImport(String processID, boolean fCloseContextOpenAfterImport) {
        this(processID);
    	m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }
    
    /**
     * Instantiates a new hapmap import.
     */
    public HapMapImport(String processID, boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this(processID);
    	m_fCloseContextAfterImport = fCloseContextAfterImport;
    	m_fAllowNewAssembly = fAllowNewAssembly;
    }

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 6)
			throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, HapMap file, and assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

		File mainFile = new File(args[4]);
		if (!mainFile.exists() || mainFile.length() == 0)
			throw new Exception("File " + args[4] + " is missing or empty!");

		int mode = 0;
		try
		{
			mode = Integer.parseInt(args[5]);
		}
		catch (Exception e)
		{
			LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
		}
		new HapMapImport().importToMongo(args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), args[5], mode);
	}

	/**
	 * Import to mongo.
	 *
	 * @param sModule the module
	 * @param sProject the project
	 * @param sRun the run
	 * @param sTechnology the technology
	 * @param mainFileUrl the main file URL
	 * @param assemblyName the assembly name
	 * @param importMode the import mode
	 * @return a project ID if it was created by this method, otherwise null
	 * @throws Exception the exception
	 */
	public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, URL mainFileUrl, String assemblyName, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID);
        if (progress == null)
            progress = new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		progress.setPercentageEnabled(false);		

		FeatureReader<RawHapMapFeature> reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), new RawHapMapCodec(), false);
		GenericXmlApplicationContext ctx = null;
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				try
				{
					ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
				}
				catch (BeanDefinitionStoreException fnfe)
				{
					LOG.warn("Unable to find applicationContext-data.xml. Now looking for applicationContext.xml", fnfe);
					ctx = new GenericXmlApplicationContext("applicationContext.xml");
				}

				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(sModule);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
			}

			if (m_processID == null)
				m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();

			GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && project.getPloidyLevel() != 2)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + 2 + ") data!");

            cleanupBeforeImport(mongoTemplate, project, importMode, sRun);

			Integer createdProject = null;
			// create project if necessary
			if (project == null || importMode == 2)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setOrigin(2 /* Sequencing */);
				project.setTechnology(sTechnology);
				createdProject = project.getId();
			}
			project.setPloidyLevel(2);

            HashMap<String, String> existingVariantIDs;
			Assembly assembly = mongoTemplate.findOne(new Query(Criteria.where(Assembly.FIELDNAME_NAME).is(assemblyName)), Assembly.class);
			if (assembly == null) {
				if ("".equals(assemblyName) || m_fAllowNewAssembly) {
					assembly = new Assembly("".equals(assemblyName) ? 0 : AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(Assembly.class)));
					assembly.setName(assemblyName);
					mongoTemplate.save(assembly);
					existingVariantIDs = new HashMap<>();
				}
				else
					throw new Exception("Assembly \"" + assemblyName + "\" not found in database. Supported assemblies are " + StringUtils.join(mongoTemplate.findDistinct(Assembly.FIELDNAME_NAME, Assembly.class, String.class), ", "));
			}
			else
				existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false, assembly.getId());
			
			if (!project.getVariantTypes().contains(Type.SNP.toString()))
				project.getVariantTypes().add(Type.SNP.toString());

			// loop over each variation
			long count = 0;
			String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());
			int nNumberOfVariantsToSaveAtOnce = 1;
			ArrayList<VariantData> unsavedVariants = new ArrayList<VariantData>();
			ArrayList<VariantRunData> unsavedRuns = new ArrayList<VariantRunData>();
			HashMap<String /*individual*/, GenotypingSample> previouslyCreatedSamples = new HashMap<>();
			Iterator<RawHapMapFeature> it = reader.iterator();
			progress.addStep("Processing variant lines");
			progress.moveToNextStep();
			int nMaxChunkSize = existingVariantIDs.size() == 0 ? 10000 /*inserted at once*/ : 50000 /*saved one by one*/;
			final MongoTemplate finalMongoTemplate = mongoTemplate;
            Thread asyncThread = null;
			while (it.hasNext())
			{
				RawHapMapFeature hmFeature = it.next();
				try
				{
					String variantId = null;
					for (String variantDescForPos : getIdentificationStrings(Type.SNP.toString(), hmFeature.getChr(), (long) hmFeature.getStart(), hmFeature.getName().length() == 0 ? null : Arrays.asList(new String[] {hmFeature.getName()})))
					{
						variantId = existingVariantIDs.get(variantDescForPos);
						if (variantId != null)
							break;
					}
					VariantData variant = variantId == null ? null : mongoTemplate.findById(variantId, VariantData.class);
					if (variant == null)
						variant = new VariantData(hmFeature.getName() != null && hmFeature.getName().length() > 0 ? ((ObjectId.isValid(hmFeature.getName()) ? "_" : "") + hmFeature.getName()) : (generatedIdBaseString + String.format(String.format("%09x", count))));

					VariantRunData runToSave = addHapMapDataToVariant(mongoTemplate, variant, assembly.getId(), hmFeature, project, sRun, previouslyCreatedSamples);

					if (!project.getSequences(assembly.getId()).contains(hmFeature.getChr()))
						project.getSequences(assembly.getId()).add(hmFeature.getChr());

					int alleleCount = hmFeature.getAlleles().length;
					project.getAlleleCounts().add(variant.getKnownAlleleList().size());	// it's a TreeSet so it will only be added if it's not already present
					if (alleleCount > 2)
						LOG.warn("Variant " + variant.getId() + " (" + variant.getReferencePosition(assembly.getId()).getSequence() + ":" + variant.getReferencePosition(assembly.getId()).getStartSite() + ") has more than 2 alleles!");
					
					if (variant.getKnownAlleleList().size() > 0)
					{	// we only import data related to a variant if we know its alleles
						if (!unsavedVariants.contains(variant))
							unsavedVariants.add(variant);
						if (!unsavedRuns.contains(runToSave))
							unsavedRuns.add(runToSave);
					}

					if (count == 0)
					{
						nNumberOfVariantsToSaveAtOnce = hmFeature.getSampleIDs().length == 0 ? nMaxChunkSize : Math.max(1, nMaxChunkSize / hmFeature.getSampleIDs().length);
						LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);
					}
					if (count % nNumberOfVariantsToSaveAtOnce == 0)
					{
                        List<VariantData> finalUnsavedVariants = unsavedVariants;
                        List<VariantRunData> finalUnsavedRuns = unsavedRuns;
                        
	                    Thread insertionThread = new Thread() {
	                        @Override
	                        public void run() {
	                        	persistVariantsAndGenotypes(existingVariantIDs, finalMongoTemplate, finalUnsavedVariants, finalUnsavedRuns); 
	                        }
	                    };

	                    if (asyncThread == null)
                        {	// every second insert is run asynchronously for better speed
//                        	System.out.println("async");
                        	asyncThread = insertionThread;
                        	asyncThread.start();
                        }
                        else
                        {
//                        	System.out.println("sync");
                        	insertionThread.run();
                        	asyncThread.join();	// make sure previous thread has executed before going further
                        	asyncThread = null;
                        }
                        
	                    unsavedVariants = new ArrayList<>();
	                    unsavedRuns = new ArrayList<>();

						progress.setCurrentStepProgress(count);
						if (count > 0)
						{
							String info = count + " lines processed"/*"(" + (System.currentTimeMillis() - before) / 1000 + ")\t"*/;
							LOG.debug(info);
						}
					}
					count++;
				}
				catch (Exception e)
				{
					throw new Exception("Error occured importing variant number " + (count + 1) + " (" + Type.SNP.toString() + ":" + hmFeature.getChr() + ":" + hmFeature.getStart() + ")", e);
				}
			}
			reader.close();

			persistVariantsAndGenotypes(existingVariantIDs, finalMongoTemplate, unsavedVariants, unsavedRuns);

			// save project data
			if (!project.getRuns().contains(sRun))
				project.getRuns().add(sRun);
			mongoTemplate.save(project);	// always save project before samples otherwise the sample cleaning procedure in MgdbDao.prepareDatabaseForSearches may remove them if called in the meantime
            mongoTemplate.insert(previouslyCreatedSamples.values(), GenotypingSample.class);

			LOG.info("HapMapImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");

			progress.addStep("Preparing database for searches");
			progress.moveToNextStep();
			MgdbDao.prepareDatabaseForSearches(mongoTemplate);
			progress.markAsComplete();
			return createdProject;
		}
		finally
		{
			if (m_fCloseContextOpenAfterImport && ctx != null)
				ctx.close();

			reader.close();
		}
	}

	/**
	 * Adds the hap map data to variant.
	 *
	 * @param mongoTemplate the mongo template
	 * @param variantToFeed the variant to feed
	 * @param nAssemblyId the assembly id
	 * @param hmFeature the hm feature
	 * @param project the project
	 * @param runName the run name
	 * @param usedSamples the used samples
	 * @return the variant run data
	 * @throws Exception the exception
	 */
	static private VariantRunData addHapMapDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, Integer nAssemblyId,RawHapMapFeature hmFeature, GenotypingProject project, String runName, Map<String /*individual*/, GenotypingSample> usedSamples) throws Exception
	{
		// mandatory fields
		if (variantToFeed.getType() == null)
			variantToFeed.setType(Type.SNP.toString());
		else if (!variantToFeed.getType().equals(Type.SNP.toString()))
			throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());

		if (variantToFeed.getReferencePosition(nAssemblyId) == null)	// otherwise we leave it as it is (had some trouble with overridden end-sites)
			variantToFeed.setReferencePosition(nAssemblyId, new ReferencePosition(hmFeature.getChr(), hmFeature.getStart(), (long) hmFeature.getEnd()));
		
		// take into account ref and alt alleles (if it's not too late)
		if (variantToFeed.getKnownAlleleList().size() == 0)
			variantToFeed.setKnownAlleleList(Arrays.asList(hmFeature.getAlleles()));

		VariantRunData run = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
		String[] individuals = hmFeature.getSampleIDs();	// don't do this inside the loop as it's an expensive operation
			
		// genotype fields
		for (int i=0; i<hmFeature.getGenotypes().length; i++)
		{
			String genotype = hmFeature.getGenotypes()[i].toUpperCase(), gtCode = null;
			if (genotype.length() == 1 && iupacCodeConversionMap.containsKey(genotype))
				genotype = iupacCodeConversionMap.get(genotype);	// it's a IUPAC code, let's convert it to a pair of bases
			else if ("NA".equals(genotype))
				genotype = "NN";

			if (!"NN".equals(genotype) && genotype.length() == 2)
			{
				String allele1 = "" + genotype.charAt(0);
				String allele2 = "" + genotype.charAt(1);

				int firstAlleleIndex = variantToFeed.getKnownAlleleList().indexOf(allele1);
				if (firstAlleleIndex == -1 && validNucleotides.contains(allele1))
				{
					firstAlleleIndex = variantToFeed.getKnownAlleleList().size();
					variantToFeed.getKnownAlleleList().add(allele1);
				}
				int secondAlleleIndex = variantToFeed.getKnownAlleleList().indexOf(allele2);
				if (secondAlleleIndex == -1 && validNucleotides.contains(allele2))
				{
					secondAlleleIndex = variantToFeed.getKnownAlleleList().size();
					variantToFeed.getKnownAlleleList().add(allele2);
				}
				gtCode = firstAlleleIndex <= secondAlleleIndex ? (firstAlleleIndex + "/" + secondAlleleIndex) : (secondAlleleIndex + "/" + firstAlleleIndex);
			}
			if (!"NN".equals(genotype) && (gtCode == null || !gtCode.matches("([0-9])([0-9])*/([0-9])([0-9])*")))
			{
				gtCode = null;
				LOG.warn("Ignoring invalid HapMap genotype \"" + genotype + "\" for variant " + variantToFeed.getId() + " and individual " + individuals[i]);
			}
			
			if (gtCode == null)
				continue;	// we don't add missing genotypes

			if (!usedSamples.containsKey(individuals[i]))	// we don't want to persist each sample several times
			{
                Individual ind = mongoTemplate.findById(individuals[i], Individual.class);
                if (ind == null) {	// we don't have any population data so we don't need to update the Individual if it already exists
                    ind = new Individual(individuals[i]);
                    mongoTemplate.save(ind);
                }

                int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                usedSamples.put(individuals[i], new GenotypingSample(sampleId, project.getId(), run.getRunName(), individuals[i]));	// add a sample for this individual to the project
            }			

            run.setSampleGenotype(usedSamples.get(individuals[i]).getId(), gtCode); // gt and ai fields separate, genotypes grouped by sample, ai grouped by field (new4 struct) --> the best solution!
		}
		
        run.setKnownAlleleList(variantToFeed.getKnownAlleleList());
        run.setReferencePositions(variantToFeed.getReferencePositions());
        run.setType(variantToFeed.getType());
		return run;
	}
}
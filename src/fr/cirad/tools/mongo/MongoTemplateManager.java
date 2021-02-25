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
package fr.cirad.tools.mongo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ServerDescription;

import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.Database;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.tools.AppConfig;
import fr.cirad.tools.Helper;
//import fr.cirad.tools.mongo.MongoTemplateManager.ModuleAction;
import htsjdk.tribble.index.Index;
import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.index.tabix.TabixIndexCreator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;

/**
 * The Class MongoTemplateManager.
 */
@Component
public class MongoTemplateManager implements ApplicationContextAware {

    /**
     * The Constant LOG.
     */
    static private final Logger LOG = Logger.getLogger(MongoTemplateManager.class);
    
    static private final String EUTILS_TAX_SEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=taxonomy&retmode=json&id=";
	// static private final String EUTILS_TAX_SEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=taxonomy&retmode=json&term="; // this one may be used to find out taxid from scientific name

    private static MongoTemplate commonsTemplate;
    
    /**
     * The application context.
     */
    static private ApplicationContext applicationContext;

    /**
     * The template map.
     */
    static private Map<String, MongoTemplate> templateMap = new TreeMap<>();
    
//    /**
//     * The taxon map.
//     */
//    static private Map<String, String> taxonMap = new TreeMap<>();
//
//    /**
//     * The public databases.
//     */
//    static private Set<String> publicDatabases = new TreeSet<>();
//
//    /**
//     * The hidden databases.
//     */
//    static private List<String> hiddenDatabases = new ArrayList<>();

    /**
     * The mongo clients.
     */
    static private Map<String, MongoClient> mongoClients = new HashMap<>();

    /**
     * The resource bundle
     */
    static private ResourceBundle dataSourceBundle;

    /**
     * The datasource resource (properties filename)
     */
    static private String resource = "datasources";

    /**
     * The expiry prefix.
     */
    static private String EXPIRY_PREFIX = "_ExpiresOn_";

    /**
     * The temp export prefix.
     */
    static public String TEMP_COLL_PREFIX = "tmpVar_";

    /**
     * The dot replacement string.
     */
    static final public String DOT_REPLACEMENT_STRING = "\\[dot\\]";

    /**
     * store ontology terms
     */
    static private Map<String, String> ontologyMap;

    /**
     * The app config.
     */
    @Autowired private AppConfig appConfig;
    
    private static final List<String> addressesConsideredLocal = Arrays.asList("127.0.0.1", "localhost");

    /**
     * The resource control.
     */
    private static final Control resourceControl = new ResourceBundle.Control() {
        @Override
        public boolean needsReload(String baseName, java.util.Locale locale, String format, ClassLoader loader, ResourceBundle bundle, long loadTime) {
            return true;
        }

        @Override
        public long getTimeToLive(String baseName, java.util.Locale locale) {
            return 0;
        }
    };

    /* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
     */
    @Override
    public void setApplicationContext(ApplicationContext ac) throws BeansException {
        initialize(ac);
        
    	if (commonsTemplate == null)
        	throw new Error("No commonsTemplate entry was found in applicationContext-data.xml");
    	
        String serverCleanupCSV = appConfig.dbServerCleanup();
        List<String> authorizedCleanupServers = serverCleanupCSV == null ? null : Arrays.asList(serverCleanupCSV.split(","));

        // we do this cleanup here because it only happens when the webapp is being (re)started
        for (Database db : commonsTemplate.findAll(Database.class)) { 
//        for (String sModule : templateMap.keySet()) {
        	MongoClient client = mongoClients.get(getModuleHost(db.getId()));
        	if (client == null)
        		continue;
         	List<ServerDescription> serverDescriptions = client.getClusterDescription().getServerDescriptions();
            MongoTemplate mongoTemplate = templateMap.get(db.getId());
            if (authorizedCleanupServers == null || (serverDescriptions.size() == 1 && authorizedCleanupServers.contains(serverDescriptions.get(0).getAddress().toString()))) {
                for (String collName : mongoTemplate.getCollectionNames()) {
                    if (collName.startsWith(TEMP_COLL_PREFIX)) {
                        mongoTemplate.dropCollection(collName);
                        LOG.debug("Dropped collection " + collName + " in module " + db.getName());
                    }
                }
            }
        }
    }

    public static Map<String, MongoTemplate> getTemplateMap() {
        return templateMap;
    }

    /**
     * Initialize.
     *
     * @param ac the app-context
     * @throws BeansException the beans exception
     */
    static public void initialize(ApplicationContext ac) throws BeansException {
    	if (applicationContext != null)
    		return;	// already initialized
    	
        applicationContext = ac;
        while (applicationContext.getParent() != null) /* we want the root application-context */
            applicationContext = applicationContext.getParent();

        loadDataSources();
    }
    
    static public void clearExpiredDatabases() {
		for (Database db : MongoTemplateManager.getCommonsTemplate().find(new Query(Criteria.where("_id").in(MongoTemplateManager.getAvailableModules(null))), Database.class))
			if (db.getName().contains(EXPIRY_PREFIX)) {
			    long expiryDate = Long.valueOf((db.getName().substring(db.getName().lastIndexOf(EXPIRY_PREFIX) + EXPIRY_PREFIX.length())));
			    if (System.currentTimeMillis() > expiryDate) {
			        if (removeDataSource(db.getId(), true))
			        	LOG.info("Removed expired datasource entry " + db.getId() + " and temporary database " + db.getName());
			    }
			}
    }

    static public void parseTaxInfoAndAddToDB(String taxInfo, Database db) {
    	if (taxInfo == null)
    		return;

		String[] ncbiTaxonIdNameAndSpecies = taxInfo.split(":");
		if (ncbiTaxonIdNameAndSpecies.length != 3)
			LOG.warn("Unparseable taxon info: " + taxInfo);
		else {
			try {
				db.setTaxid(Integer.parseInt(ncbiTaxonIdNameAndSpecies[0]));
			}
			catch (NumberFormatException nfe) {
				LOG.warn("Unparseable taxid info: " + ncbiTaxonIdNameAndSpecies[0]);
			}
			if (ncbiTaxonIdNameAndSpecies[1] != null)
				db.setTaxon(ncbiTaxonIdNameAndSpecies[1]);
			if (ncbiTaxonIdNameAndSpecies[2] != null)
				db.setSpecies(ncbiTaxonIdNameAndSpecies[2]);
		}
    }

    static public void fetchTaxonInfoForDB(Database db) {
    	if (db.getTaxid() == null)
    		return;

		try {
			Map taxInfo = new ObjectMapper().readValue(new URL(EUTILS_TAX_SEARCH_URL + db.getTaxid()), Map.class);
			Map taxonResult = (Map) ((Map) taxInfo.get("result")).get("" + db.getTaxid());
			db.setTaxon((String) taxonResult.get("scientificname"));
			db.setSpecies("species".equals(taxonResult.get("rank")) ? db.getTaxon() : null);
		} catch (IOException e) {
			LOG.warn("Unable to parse EUTILS response searching info for taxon " + db.getTaxid(), e);
		}
    }
    
    /**
     * Load data sources.
     */
    static public void loadDataSources() {
        templateMap.clear();
        mongoClients.clear();
//        publicDatabases.clear();
//        hiddenDatabases.clear();
//        try {
            mongoClients = applicationContext.getBeansOfType(MongoClient.class);
            
            commonsTemplate = applicationContext.getBeansOfType(MongoTemplate.class).get("commonsTemplate");
//            commonsTemplate.dropCollection(Database.class);	/*FIXME: remove me after tests*/

            try {	// see if we need to migrate datasources from file (old configuration mode) into DB (new configuration mode)
	            dataSourceBundle = ResourceBundle.getBundle(resource, resourceControl);
	            Enumeration<String> bundleKeys = dataSourceBundle.getKeys();
	            while (bundleKeys.hasMoreElements()) {
	                String key = bundleKeys.nextElement();
	                String[] datasourceInfo = dataSourceBundle.getString(key).split(",");
	
	                if (datasourceInfo.length < 2) {
	                    LOG.error("Unable to deal with datasource info for key " + key + ". Datasource definition requires at least 2 comma-separated strings: mongo host bean name (defined in Spring application context) and database name");
	                    continue;
	                }
	
	                boolean fHidden = key.endsWith("*"), fPublic = key.startsWith("*");
	                String cleanKey = key.replaceAll("\\*", "");
	                if (cleanKey.length() == 0) {
	                	LOG.warn("Skipping unnamed datasource");
	                	continue;
	                }
	
	                if (templateMap.containsKey(cleanKey)) {
	                    LOG.error("Datasource " + cleanKey + " already exists!");
	                    continue;
	                }
	                
	                Database db = commonsTemplate.findById(cleanKey, Database.class);
	                if (db == null) {
	                	LOG.info("Adding database to commons: " + cleanKey);
	                	db = new Database(cleanKey);
	                	db.setPublic(fPublic);
	                	db.setHidden(fHidden);
	                	db.setHost(datasourceInfo[0]);
	                	db.setName(datasourceInfo[1]);
	                	if (datasourceInfo.length > 2) {
	                		parseTaxInfoAndAddToDB(datasourceInfo[2], db);
	
	//                			db.setSpecies(ncbiTaxonIdNameAndSpecies[2]);
	//	                		if (ncbiTaxonIdNameAndSpecies.length > 1)
	//	                			db.setTaxon(ncbiTaxonIdNameAndSpecies[1].isEmpty() ? db.getSpecies() : ncbiTaxonIdNameAndSpecies[1]);
	//	                		if (ncbiTaxonIdNameAndSpecies.length > 0)
	//	                			db.setTaxon(ncbiTaxonIdNameAndSpecies[1].isEmpty() ? db.getSpecies() : ncbiTaxonIdNameAndSpecies[1]);
	//	                		try {
	//								Map taxInfo = new ObjectMapper().readValue(new URL(EUTILS_TAX_SEARCH_URL + URLEncoder.encode(datasourceInfo[2], "UTF-8")), Map.class);
	//								if (!"1".equals(((Map) taxInfo.get("esearchresult")).get("count")))
	//									LOG.warn("Unable to find a single taxon id for name: " + datasourceInfo[2]);
	//								else
	//									db.setTaxid(Integer.parseInt(((List<String>) ((Map) taxInfo.get("esearchresult")).get("idlist")).get(0)));	// only 1 match, we're being lucky!
	//							} catch (IOException e) {
	//								LOG.warn("Unable to parse EUTILS response searching taxon id for name: " + datasourceInfo[2], e);
	//							}
	                		}
	                	}
	            		commonsTemplate.save(db);
	                }
	            
            	try {
            		File f = new ClassPathResource("/" + resource + ".properties").getFile();
            		f.renameTo(new File(f.getAbsolutePath() + ".bak"));
		            LOG.info("Datasources successfuly migrated from file to commons database");
				} catch (IOException e) {
					LOG.error("Error renaming " + resource + ".properties after migration into commons database");
				}
            }
            catch (MissingResourceException ignored) {}

            for (Database db : commonsTemplate.findAll(Database.class))
				try {
					templateMap.put(db.getId(), createMongoTemplate(db.getHost(), db.getName()));
				}
	            catch (UnknownHostException e) {
                  LOG.warn("Unable to create MongoTemplate for module " + db.getId() + " (no such host)");
	            }
	            catch (Exception e) {
	              LOG.warn("Unable to create MongoTemplate for module " + db.getId(), e);
	            }
//        } catch (MissingResourceException mre) {
//            LOG.error("Unable to find file " + resource + ".properties, you may need to adjust your classpath", mre);
//        }
    }

    /**
     * Creates the mongo template.
     *
     * @param sHost the host
     * @param sDbName the db name
     * @return the mongo template
     * @throws Exception the exception
     */
    public static MongoTemplate createMongoTemplate(String sHost, String sDbName) throws Exception {
        MongoClient client = mongoClients.get(sHost);
        if (client == null)
            throw new IOException("Unknown host: " + sHost);

        MongoTemplate mongoTemplate = new MongoTemplate(client, sDbName);
        ((MappingMongoConverter) mongoTemplate.getConverter()).setMapKeyDotReplacement(DOT_REPLACEMENT_STRING);
		mongoTemplate.getDb().runCommand(new BasicDBObject("profile", 0));
//		if (Helper.estimDocCount(mongoTemplate, Assembly.class) == 0) {
//			Assembly defaultAssembly = new Assembly(0);
//			mongoTemplate.save(defaultAssembly);
//			LOG.info("Default assembly created for module " + sDbName);
//		}

        return mongoTemplate;
    }
    
    public static void invokeSetters(Map<String, Object> fieldNamesAndValues, Object pojo) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		HashMap<String, Method> setterMap = new HashMap<>();
		for (Method m : Database.class.getDeclaredMethods())
			if (m.getName().startsWith("set") && m.getParameterCount() == 1)
				setterMap.put(m.getName().substring(3).toLowerCase(), m);
		
		if (fieldNamesAndValues != null)
			for (String fieldName : fieldNamesAndValues.keySet()) {
				Method setter = setterMap.get(fieldName);
				if (setter != null) {
					Class<?> paramType = setter.getParameters()[0].getType();
					try {
						Method valueOfMethod = paramType.getDeclaredMethod("valueOf", String.class);
						setter.invoke(pojo, valueOfMethod.invoke(null /*this is meant to be the instance but null is OK because we're calling a static method */, (String) fieldNamesAndValues.get(fieldName)));
					}
					catch (NoSuchMethodException nsme) {
						setter.invoke(pojo, fieldNamesAndValues.get(fieldName));
					}
				}
			}
    }
    
	public static Database createDataSource(String sModule, String sHost, boolean fPublic, boolean fHidden, Map<String, Object> customFields, Long expiryDate) throws Exception {
        if (commonsTemplate.exists(new Query(Criteria.where("_id").is(sModule)), Database.class))
        {
        	LOG.warn("Tried to create a module that already exists: " + sModule);
        	return null;
        }

		Database db = new Database(sModule);
		db.setHost(sHost);
		db.setPublic(fPublic);
		db.setHidden(fHidden);

		invokeSetters(customFields, db);
		fetchTaxonInfoForDB(db);

		int nRetries = 0;
        while (nRetries < 100)
        {
            String sIndexForModule = nRetries == 0 ? "" : ("_" + nRetries);
            String sDbName = "mgdb3_" + sModule + sIndexForModule + (expiryDate == null ? "" : (EXPIRY_PREFIX + expiryDate));
            MongoTemplate mongoTemplate = createMongoTemplate(sHost, sDbName);
            if (mongoTemplate.getCollectionNames().size() > 0)
                nRetries++;	// DB already exists, let's try with a different DB name
            else
            {
                db.setName(sDbName);
                try {
                	MongoTemplateManager.getCommonsTemplate().insert(db);
                	templateMap.put(sModule, mongoTemplate);
                	return db;
                }
                catch (Exception e) {
                	LOG.error("Error creating database", e);
                	return null;
                }
            }
        }
        throw new Exception("Unable to create a unique name for datasource " + sModule + " after " + nRetries + " retries");
	}

	public static boolean updateDataSource(String sModule, boolean fPublic, boolean fHidden, Map<String, Object> customFields) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		Database db = commonsTemplate.findById(sModule, Database.class);
		if (db == null)
			return false;

		db.setHidden(fHidden);
		db.setPublic(fPublic);
		invokeSetters(customFields, db);
		fetchTaxonInfoForDB(db);
		commonsTemplate.save(db);
		return true;
	}

//    public enum ModuleAction {
//    	CREATE, UPDATE_STATUS, DELETE;
//    }
//    
//    /**
//     * Saves or updates a data source.
//     *
//     * @param action the action to perform on the module
//     * @param sModule the module, with a leading * if public and/or a trailing * if hidden
//     * @param public flag telling whether or not the module shall be public, ignored for deletion
//	 * @param hidden flag telling whether or not the module shall be hidden, ignored for deletion
//     * @param sHost the host, only used for creation
//     * @param ncbiTaxonIdNameAndSpecies id and scientific name of the ncbi taxon (colon-separated), optional, ignored for deletion
//     * @param expiryDate the expiry date, only used for creation
//     * @throws Exception the exception
//     */
//    synchronized static public boolean saveOrUpdateDataSource(ModuleAction action, String sModule, boolean fPublic, boolean fHidden, String sHost, String ncbiTaxonIdNameAndSpecies, Long expiryDate) throws Exception
//    {	// as long as we keep all write operations in a single synchronized method, we should be safe
//    	if (get(sModule) == null) {
//    		if (!action.equals(ModuleAction.CREATE))
//    			throw new Exception("Module " + sModule + " does not exist!");
//    	}
//    	else if (action.equals(ModuleAction.CREATE))
//    		throw new Exception("Module " + sModule + " already exists!");
//    	
//    	FileOutputStream fos = null;
//        File f = new ClassPathResource("/" + resource + ".properties").getFile();
//    	FileReader fileReader = new FileReader(f);
//        Properties properties = new Properties();
//        properties.load(fileReader);
//        
//    	try
//    	{
//    		if (action.equals(ModuleAction.DELETE))
//    		{
//    	        String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
//                if (!properties.containsKey(sModuleKey))
//                {
//                	LOG.warn("Module could not be found in datasource.properties: " + sModule);
//                	return false;
//                }
//                properties.remove(sModuleKey);
//                fos = new FileOutputStream(f);
//                properties.store(fos, null);
//                return true;
//    		}
//	        else if (action.equals(ModuleAction.CREATE))
//	        {
//	            int nRetries = 0;
//		        while (nRetries < 100)
//		        {
//		            String sIndexForModule = nRetries == 0 ? "" : ("_" + nRetries);
//		            String sDbName = "mgdb2_" + sModule + sIndexForModule + (expiryDate == null ? "" : (EXPIRY_PREFIX + expiryDate));
//		            MongoTemplate mongoTemplate = createMongoTemplate(sHost, sDbName);
//		            if (mongoTemplate.getCollectionNames().size() > 0)
//		                nRetries++;	// DB already exists, let's try with a different DB name
//		            else
//		            {
//		                if (properties.containsKey(sModule) || properties.containsKey("*" + sModule) || properties.containsKey(sModule + "*") || properties.containsKey("*" + sModule + "*"))
//		                {
//		                	LOG.warn("Tried to create a module that already exists in datasource.properties: " + sModule);
//		                	return false;
//		                }
//		                String sModuleKey = (fPublic ? "*" : "") + sModule + (fHidden ? "*" : "");
//		                if (ncbiTaxonIdNameAndSpecies != null)
//		                	setTaxon(sModule, ncbiTaxonIdNameAndSpecies);
//		                properties.put(sModuleKey, sHost + "," + sDbName + "," + (ncbiTaxonIdNameAndSpecies == null ? "" : ncbiTaxonIdNameAndSpecies));
//		                fos = new FileOutputStream(f);
//		                properties.store(fos, null);
//
//		                templateMap.put(sModule, mongoTemplate);
//		                if (fPublic)
//		                    publicDatabases.add(sModule);
//		                if (fHidden)
//		                    hiddenDatabases.add(sModule);
//		                return true;
//		            }
//		        }
//		        throw new Exception("Unable to create a unique name for datasource " + sModule + " after " + nRetries + " retries");
//	        }
//	        else if (action.equals(ModuleAction.UPDATE_STATUS))
//	        {
//	        	String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
//                if (!properties.containsKey(sModuleKey))
//                {
//                	LOG.warn("Tried to update a module that could not be found in datasource.properties: " + sModule);
//                	return false;
//                }
//                String[] propValues = ((String) properties.get(sModuleKey)).split(",");
//                properties.remove(sModuleKey);
//                if (ncbiTaxonIdNameAndSpecies == null && getTaxonId(sModule) != null)
//                {
//                	String taxonName = getTaxonName(sModule), species = getSpecies(sModule);
//                	ncbiTaxonIdNameAndSpecies = getTaxonId(sModule) + ":" + (species != null && species.equals(taxonName) ? "" : taxonName) + ":" + (species != null ? species : "");
//                }
//                properties.put((fPublic ? "*" : "") + sModule + (fHidden ? "*" : ""), propValues[0] + "," + propValues[1] + "," + ncbiTaxonIdNameAndSpecies);
//                fos = new FileOutputStream(f);
//                properties.store(fos, null);
//                
//                if (fPublic)
//                    publicDatabases.add(sModule);
//                else
//                	publicDatabases.remove(sModule);
//                if (fHidden)
//                    hiddenDatabases.add(sModule);
//                else
//                	hiddenDatabases.remove(sModule);
//	        	return true;
//	        }
//	        else
//	        	throw new Exception("Unknown ModuleAction: " + action);
//        }
//    	catch (IOException ex)
//    	{
//            LOG.warn("Failed to update datasource.properties for action " + action + " on " + sModule, ex);
//            return false;
//        }
//    	finally
//    	{
//            try 
//            {
//           		fileReader.close();
//            	if (fos != null)
//            		fos.close();
//            } 
//            catch (IOException ex)
//            {
//                LOG.debug("Failed to close FileReader", ex);
//            }
//        }
//    }

    /**
     * Removes the data source.
     *
     * @param sModule the module
     * @param fAlsoDropDatabase whether or not to also drop database
     */
    static public boolean removeDataSource(String sModule, boolean fAlsoDropDatabase)
    {
        try
        {
            String key = sModule.replaceAll("\\*", "");
            commonsTemplate.remove(new Query(Criteria.where("_id").is(sModule)), Database.class);

            if (fAlsoDropDatabase)
                templateMap.get(key).getDb().drop();
            templateMap.remove(key);
//            publicDatabases.remove(key);
//            hiddenDatabases.remove(key);
            return true;
        }
        catch (Exception ex)
        {
            LOG.warn("Failed to remove datasource " + sModule, ex);
            return false;
        }
    }

    /**
     * fill the ontology map
     *
     * @param newOntologyMap
     */
    public static void setOntologyMap(Map<String, String> newOntologyMap) {
        ontologyMap = newOntologyMap;
    }

    /**
     * getter for ontology map
     *
     * @return
     */
    public static Map<String, String> getOntologyMap() {
        return ontologyMap;
    }

    /**
     * Gets the host names.
     *
     * @return the host names
     */
    static public Set<String> getHostNames() {
        return mongoClients.keySet();
    }

    /**
     * Gets the.
     *
     * @param module the module
     * @return the mongo template
     */
    static public MongoTemplate get(String module) {
        return templateMap.get(module);
    }

    /**
     * Gets the public database names.
     *
     * @return the public database names
     */
    static public Collection<String> getPublicDatabases() {
        return commonsTemplate.findDistinct(new Query(Criteria.where(Database.FIELDNAME_PUBLIC).is(true)), "_id", Database.class, String.class);
    }

    static public void dropAllTempColls(String token) {
    	if (token == null)
    		return;
    	
        MongoCollection<org.bson.Document> tmpColl;
        String tempCollName = MongoTemplateManager.TEMP_COLL_PREFIX + Helper.convertToMD5(token);
        for (String module : MongoTemplateManager.getTemplateMap().keySet()) {
            // drop all temp collections associated to this token
            tmpColl = templateMap.get(module).getCollection(tempCollName);
//            LOG.debug("Dropping " + module + "." + tempCollName + " from dropAllTempColls");
            tmpColl.drop();
        }
    }

    /**
     * Gets the available modules.
     *
     * @param nMgdbVersion the module version (null for any version)
     * @return the available modules
     */
    static public Collection<String> getAvailableModules(Integer nMgdbVersion) {
    	if (nMgdbVersion == null)
    		return templateMap.keySet();
    	
    	if (nMgdbVersion < 1 || nMgdbVersion > 3)
    		return new ArrayList<>();
    	
    	return commonsTemplate.findDistinct(new Query(Criteria.where(Database.FIELDNAME_NAME).regex("^mgdb" + (nMgdbVersion == 1 ? "" : nMgdbVersion) + "_.*")), "_id", Database.class, String.class);
    }

    /**
     * Checks if is module public.
     *
     * @param sModule the module
     * @return true, if is module public
     */
    static public boolean isModulePublic(String sModule) {
    	return commonsTemplate.findById(sModule, Database.class).isPublic();
    }

    /**
     * Checks if is module hidden.
     *
     * @param sModule the module
     * @return true, if is module hidden
     */
    static public boolean isModuleHidden(String sModule) {
    	return commonsTemplate.findById(sModule, Database.class).isHidden();
    }

//	public void saveRunsIntoProjectRecords()
//	{
//		for (String module : getAvailableModules())
//		{
//			MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
//			for (GenotypingProject proj : mongoTemplate.findAll(GenotypingProject.class))
//				if (proj.getRuns().size() == 0)
//				{
//					boolean fRunAdded = false;
//					for (String run : (List<String>) mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(VariantData.class)).distinct(VariantData.FIELDNAME_PROJECT_DATA + "." + proj.getId() + "." + Run.RUNNAME))
//						if (!proj.getRuns().contains(run))
//						{
//							proj.getRuns().add(run);
//							LOG.info("run " + run + " added to project " + proj.getName() + " in module " + module);
//							fRunAdded = true;
//						}
//					if (fRunAdded)
//						mongoTemplate.save(proj);
//				}
//		}
//	}
    /**
     * Gets the mongo collection name.
     *
     * @param clazz the class
     * @return the mongo collection name
     */
    public static String getMongoCollectionName(Class clazz) {
        Document document = (Document) clazz.getAnnotation(Document.class);
        if (document != null) {
            return document.collection();
        }
        return clazz.getSimpleName();
    }

//	public static void setTaxon(String database, String taxon) {
//		taxonMap.put(database, taxon);
//	}
//	
//	public static Integer getTaxonId(String database) {
//		return commonsTemplate.findById(database, Database.class).getTaxid();
//	}
//	
//	public static String getTaxonName(String database) {
//		return commonsTemplate.findById(database, Database.class).getTaxon();
//	}
//	
//	public static String getSpecies(String database) {
//		return commonsTemplate.findById(database, Database.class).getSpecies();
//	}
	
    public static MongoTemplate getCommonsTemplate() {
		return commonsTemplate;
	}

    public static String getModuleHost(String sModule) {
    	return commonsTemplate.findById(sModule, Database.class).getHost();
//        Enumeration<String> bundleKeys = dataSourceBundle.getKeys();
//        while (bundleKeys.hasMoreElements()) {
//            String key = bundleKeys.nextElement();
//            
//            if (sModule.equals(key.replaceAll("\\*", ""))) {
//            	String[] datasourceInfo = dataSourceBundle.getString(key).split(",");
//            	return datasourceInfo[0];
//            }
//        }
//        return null;
    }
    
    public static boolean isModuleOnLocalHost(String sModule) {
    	List<ServerDescription> descs = mongoClients.get(getModuleHost(sModule)).getClusterDescription().getServerDescriptions();
    	for (ServerDescription desc : descs)
    		if (!addressesConsideredLocal.contains(desc.getAddress().getHost()))
    			return false;
    	return true;
    }
    
//    public static void main(String[] args) throws IOException {
//    	TabixIndexCreator tbi = new TabixIndexCreator(TabixFormat.VCF);
//    	tbi.addFeature(new VariantContextBuilder("Unknown", "chr8", 1500, 1502, Arrays.asList(Allele.create("AAC", true), Allele.create("G", false))).make(), 1);
//    	tbi.addFeature(new VariantContextBuilder("Unknown", "chr8", 1600, 1600, Arrays.asList(Allele.create("T", true), Allele.create("A", false))).make(), 2);
//    	tbi.addFeature(new VariantContextBuilder("Unknown", "chr8", 1700, 1702, Arrays.asList(Allele.create("GAT", true), Allele.create("TT", false))).make(), 3);
//    	tbi.addFeature(new VariantContextBuilder("Unknown", "chr9", 500, 502, Arrays.asList(Allele.create("AAC", true), Allele.create("G", false))).make(), 4);
//    	tbi.addFeature(new VariantContextBuilder("Unknown", "chr9", 600, 600, Arrays.asList(Allele.create("T", true), Allele.create("A", false))).make(), 5);
//    	tbi.addFeature(new VariantContextBuilder("Unknown", "chr6", 2600, 2600, Arrays.asList(Allele.create("G", true), Allele.create("C", false))).make(), 6);
//    	Index t = tbi.finalizeIndex(7);
//    	t.write(new File("/home/sempere/Bureau/projects/gigwa/test.tbi"));
//    	System.err.println(t);
//    	System.err.println("done");
//    }
}

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
package fr.cirad.mgdb.exporting;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.IntKeyMapPropertyCodecProvider;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.IndexOptions;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantDataV2;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunDataV2;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantDataV2;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.tools.ProgressIndicator;

/**
 * The Interface IExportHandler.
 */
public interface IExportHandler
{
	/** The Constant LOG. */
	static final Logger LOG = Logger.getLogger(IExportHandler.class);
	
//	static final Document projectionDoc = new Document(AbstractVariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(AbstractVariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE, 1);	
//	static final Document sortDoc = new Document(AbstractVariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(AbstractVariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE, 1);
	
//	static Document projectionDoc(Integer nAssemblyId) {
//		String posPath = nAssemblyId != null ? (VariantData.FIELDNAME_REFERENCE_POSITION + "." + nAssemblyId) : VariantDataV2.FIELDNAME_REFERENCE_POSITION;
//		return new Document(posPath + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(posPath  + "." + ReferencePosition.FIELDNAME_START_SITE, 1);	
//	}

	static final CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().register(new IntKeyMapPropertyCodecProvider()).automatic(true).build()));

	static Document sortDoc(Integer nAssemblyId) {
		String posPath = nAssemblyId != null ? (VariantData.FIELDNAME_REFERENCE_POSITION + "." + nAssemblyId) : VariantDataV2.FIELDNAME_REFERENCE_POSITION;
		return new Document(posPath  + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(posPath + "." + ReferencePosition.FIELDNAME_START_SITE, 1);
	}
	
	static final Collation collationObj = Collation.builder().numericOrdering(true).locale("en_US").build();
    
	/** The Constant nMaxChunkSizeInMb. */
	static final int nMaxChunkSizeInMb = 5;
	
	/** The Constant LINE_SEPARATOR. */
	static final String LINE_SEPARATOR = "\n";

	/**
	 * Gets the export format name.
	 *
	 * @return the export format name
	 */
	public String getExportFormatName();
	
	/**
	 * Gets the export format description.
	 *
	 * @return the export format description
	 */
	public String getExportFormatDescription();
	
	/**
	 * Gets the export archive extension.
	 *
	 * @return the export file extension.
	 */
	public String getExportArchiveExtension();
	
	/**
	 * Gets the export file content-type
	 *
	 * @return the export file content-type.
	 */
	public String getExportContentType();
	
	/**
	 * Gets the export files' extensions.
	 *
	 * @return the export files' extensions.
	 */
	public String[] getExportDataFileExtensions();
	
	/**
	 * Gets the step list.
	 *
	 * @return the step list
	 */
	public List<String> getStepList();
	
	/**
	 * Gets the supported variant types.
	 *
	 * @return the supported variant types
	 */
	public List<String> getSupportedVariantTypes();
	
	public static List<AbstractVariantData> getMarkerListWithCorrectCollation(MongoTemplate mongoTemplate, Class varClass, Query varQuery, Integer nAssemblyId, int skip, int limit) {
		varQuery.collation(org.springframework.data.mongodb.core.query.Collation.of("en_US").numericOrderingEnabled());
		varQuery.with(Sort.by(Order.asc(VariantData.FIELDNAME_REFERENCE_POSITION + (nAssemblyId != null ? "." + nAssemblyId : "") + "." + ReferencePosition.FIELDNAME_SEQUENCE), Order.asc(VariantData.FIELDNAME_REFERENCE_POSITION + (nAssemblyId != null ? "." + nAssemblyId : "") + "." + ReferencePosition.FIELDNAME_START_SITE)));
		varQuery.skip(skip).limit(limit).cursorBatchSize(limit);
		String varCollName = mongoTemplate.getCollectionName(varClass);
		try {
			return mongoTemplate.find(varQuery, varClass, varCollName);
		}
		catch (UncategorizedMongoDbException umde) {
			if (umde.getMessage().contains("Add an index")) {
				LOG.info("Creating position index with collation en_US on variants collection");
				
				MongoCollection<Document> varColl = mongoTemplate.getCollection(varCollName);
				String rpPath = nAssemblyId == null ? AbstractVariantDataV2.FIELDNAME_REFERENCE_POSITION : AbstractVariantData.FIELDNAME_REFERENCE_POSITION;
				BasicDBObject indexKeys = new BasicDBObject(rpPath + (nAssemblyId != null ? "." + nAssemblyId : "") + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(rpPath + (nAssemblyId != null ? "." + nAssemblyId : "") + "." +  ReferencePosition.FIELDNAME_START_SITE, 1);
				try {
					varColl.dropIndex(indexKeys);	// it probably exists without the collation
				}
				catch (MongoCommandException ignored)
				{}
				
				varColl.createIndex(indexKeys, new IndexOptions().collation(Collation.builder().locale("en_US").numericOrdering(true).build()));
				
				return mongoTemplate.find(varQuery, varClass, varCollName);
			}
			throw umde;
		}
	}

	public static MongoCursor<Document> getMarkerCursorWithCorrectCollation(MongoCollection<Document> varColl, Class resultType, Document varQuery, Document projection, Integer nAssemblyId, int nQueryChunkSize) {
		try {
			return varColl.find(varQuery, resultType).projection(projection).collation(collationObj).sort(sortDoc(nAssemblyId)).batchSize(nQueryChunkSize).noCursorTimeout(true).iterator();
		}
		catch (MongoQueryException mqe) {
			if (mqe.getMessage().contains("Add an index")) {
				LOG.info("Creating position index with collation en_US on variants collection");
				
				String rpPath = nAssemblyId == null ? AbstractVariantDataV2.FIELDNAME_REFERENCE_POSITION : AbstractVariantData.FIELDNAME_REFERENCE_POSITION;				
				BasicDBObject indexKeys = new BasicDBObject(rpPath + (nAssemblyId != null ? "." + nAssemblyId : "") + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(rpPath + (nAssemblyId != null ? "." + nAssemblyId : "") + "." +  ReferencePosition.FIELDNAME_START_SITE, 1);
				try {
					varColl.dropIndex(indexKeys);	// it probably exists without the collation
				}
				catch (MongoCommandException ignored)
				{}
				
				varColl.createIndex(indexKeys, new IndexOptions().collation(collationObj));
				
				return varColl.find(varQuery, resultType).projection(projection).collation(collationObj).sort(sortDoc(nAssemblyId)).batchSize(nQueryChunkSize).noCursorTimeout(true).iterator();
			}
			throw mqe;
		}
	}
	
    static void readAndWrite(MongoCursor markerCursor, ProgressIndicator progress, FileWriter warningFileWriter, boolean fV2Model, int nQueryChunkSize, LinkedHashMap<String, List<Object>> markerRunsToWrite, Thread writingThread, long variantCount) throws IOException, InterruptedException, ExecutionException {
		CompletableFuture<Void> future = null;
		LinkedHashMap<String, List<Object>> tempMarkerRunsToWrite = new LinkedHashMap<>();
		List<Object> currentMarkerRuns = new ArrayList<>();
		String varId = null, previousVarId = null;
		int nWrittenVariantCount = 0;
//		long timeReading = 0;
		while (markerCursor.hasNext()) {
//			System.out.println("reading " + nWrittenVariantCount);
//			long b4r = System.currentTimeMillis();
            if (progress.isAborted() && warningFileWriter != null) {
                warningFileWriter.close();
			    break;
            }
            
			Object aRun = markerCursor.next();
			if (fV2Model) {
				VariantRunDataV2 vrd = (VariantRunDataV2) aRun;
				varId = vrd.getId().getVariantId();
			}
			else {
				VariantRunData vrd = (VariantRunData) aRun;
				varId = vrd.getId().getVariantId();
			}
			
			if (previousVarId != null && varId != previousVarId) {
				tempMarkerRunsToWrite.put(previousVarId, currentMarkerRuns);
				currentMarkerRuns = new ArrayList<>();
			}
			
			currentMarkerRuns.add(aRun);

			if (!markerCursor.hasNext())
				tempMarkerRunsToWrite.put(varId, currentMarkerRuns);

//			long duration = System.currentTimeMillis() - b4r;
//			timeReading += duration;
//			System.out.println("read " + nWrittenVariantCount + " in " + duration + "ms");

			if (tempMarkerRunsToWrite.size() >= nQueryChunkSize || !markerCursor.hasNext()) {
				if (future != null && !future.isDone()) {
//					long b4 = System.currentTimeMillis();
					future.get();
//					long delay = System.currentTimeMillis() - b4;
//					if (delay > 100)
//						LOG.debug(progress.getProcessId() + " waited " + delay + "ms before writing variant " + nWrittenVariantCount);
				}

				markerRunsToWrite.putAll(tempMarkerRunsToWrite);	// only do this when previous execution has completed, to avoid ConcurrentModificationException
				tempMarkerRunsToWrite.clear();
				future = CompletableFuture.runAsync(writingThread);
//				writingThread.run();
			}
			
			previousVarId = varId;
			progress.setCurrentStepProgress(++nWrittenVariantCount * 100l / variantCount);
		}
		if (future != null && !future.isDone())
			future.get();
//		System.out.println("time spent reading: " + timeReading);
    }
}
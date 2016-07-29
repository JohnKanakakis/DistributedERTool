package spark;

import java.io.ByteArrayInputStream;
import java.io.File;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.aksw.limes.core.execution.engine.ExecutionEngine;
import org.aksw.limes.core.execution.engine.ExecutionEngineFactory;
import org.aksw.limes.core.execution.engine.SimpleExecutionEngine;
import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.cache.HybridCache;
import org.aksw.limes.core.io.cache.MemoryCache;
import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.aksw.limes.core.io.mapping.Mapping;
import org.aksw.limes.core.io.mapping.MemoryMapping;
import org.aksw.limes.core.io.preprocessing.Preprocessor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.hp.hpl.jena.vocabulary.OWL;

import scala.Tuple2;
import spark.io.DataReader;


public class Linker {
	
	
	public static Logger logger = LoggerFactory.getLogger(Linker.class);
	
	

	public static JavaPairRDD<String, String> run(JavaPairRDD<String, Set<List<String>>> blocks, 
			               			  final Broadcast<byte[]> planBinary_B,
			               			  final Broadcast<byte[]> configBinary_B) {
		
		final org.aksw.limes.core.io.config.Configuration config = (org.aksw.limes.core.io.config.Configuration) Utils.deserialize(configBinary_B.value());
		final KBInfo sourceKb = config.getSourceInfo();
		final KBInfo targetKb = config.getTargetInfo();
		final NestedPlan plan = (NestedPlan) Utils.deserialize(planBinary_B.getValue());
		
		final String sourceVar = sourceKb.getVar();
		final String targetVar = targetKb.getVar();
		final double thres = config.getAcceptanceThreshold();
		
		
		Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>> seqFunc = 
				new Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> resourceInfoSetPerKey, 
							Tuple2<String,Double> resourceInfo) 
					throws Exception {
						// TODO Auto-generated method stub
						resourceInfoSetPerKey.add(resourceInfo);
						return resourceInfoSetPerKey;
					}
		};
		
		Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>> combFunc = 
				new Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> resourceInfoSetPerKey_1, 
											Set<Tuple2<String,Double>> resourceInfoSetPerKey_2) 
					throws Exception {
						// TODO Auto-generated method stub
						resourceInfoSetPerKey_1.addAll(resourceInfoSetPerKey_2);
						return resourceInfoSetPerKey_1;
					}
		};
		
		
		PairFlatMapFunction<Iterator<Tuple2<String, Set<List<String>>>>, String, Tuple2<String, Double>> linkF =
				new PairFlatMapFunction<Iterator<Tuple2<String, Set<List<String>>>>, String, Tuple2<String,Double>>(){
					private static final long serialVersionUID = 1L;
					SimpleExecutionEngine engine = new SimpleExecutionEngine();
					
					
				    @Override
					public Iterable<Tuple2<String, Tuple2<String,Double>>> call(Iterator<Tuple2<String, Set<List<String>>>> blocksOfPartition) throws Exception {
						// TODO Auto-generated method stub
						ArrayList<Tuple2<String,Tuple2<String,Double>>> partitionLinks = 
								new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
						
						
						Tuple2<String, Set<List<String>>> blockPair;
						while(blocksOfPartition.hasNext()){
							blockPair = blocksOfPartition.next();
							partitionLinks.addAll(getLinksOfBlock(blockPair));
						}
						
						return partitionLinks;
					}		
					
					private List<Tuple2<String, Tuple2<String, Double>>> getLinksOfBlock(
							Tuple2<String, Set<List<String>>> blockPair) {
						// TODO Auto-generated method stub
						ArrayList<Tuple2<String,Tuple2<String,Double>>> localLinks = 
								new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
						
						int cnt1 = 0;
						int cnt2 = 0;
						String resourceId;
						String datasetId;
						for(List<String> resource : blockPair._2){
							resourceId = resource.get(0);
							datasetId = DatasetManager.getDatasetIdOfResource(resourceId);
						  	if(datasetId.equals(sourceKb.getId())){
						  		cnt1++;
						  	}else if(datasetId.equals(targetKb.getId())){
						  		cnt2++;
						  	}
						}
						
						MemoryCache sourceCache = new MemoryCache(cnt1);
					    MemoryCache targetCache = new MemoryCache(cnt2);
					    MemoryCache cache = null;
					   
						KBInfo kb = null;

						String subject;
						String predicate = null;
						String object = null;
						String value;
						
					   	for(List<String> resourceInfo : blockPair._2){
					   	
					    	subject = resourceInfo.get(0);
					    	datasetId = DatasetManager.getDatasetIdOfResource(subject);
					    	if(datasetId.equals(sourceKb.getId())){
					    		cache = sourceCache;
					    		kb = sourceKb;
					    		
					    	}else if(datasetId.equals(targetKb.getId())){
					    		cache = targetCache;
					    		kb = targetKb;
					    		
					    	}
					    	
					    	if(resourceInfo.size()%2 == 0){
					    		logger.error("malformed list "+resourceInfo);
					    		return null;
					    	}
					    	
					    	for(int i = 1; i < resourceInfo.size()-1; i = i+2){
					    		predicate = resourceInfo.get(i);
					    		
					    		if(kb.getProperties().contains(predicate)){
					    			object = DataFormatter.eliminateDataTypeFromLiteral(resourceInfo.get(i+1));
						    		if(kb.getFunctions().get(predicate).keySet().size() == 0){
						    			
										cache.addTriple(subject, predicate, object);
						    		}
									else{
										for (String propertyDub : kb.getFunctions().get(predicate).keySet()) {
											value = Preprocessor.process(object, kb.getFunctions().get(predicate).get(propertyDub));
											cache.addTriple(subject, propertyDub, value);
										}
									}
									
								}
					    	}
					    }
						   	
						if(sourceCache.size() == 0 || targetCache.size() == 0) return localLinks;
					   	
						engine.configure(sourceCache, targetCache, sourceVar, targetVar);

						Mapping verificationMapping = engine.execute(plan);

						Tuple2<String,Double> tp = null;
						
						HashMap<String, Double> targets;
						for(String source: verificationMapping.getMap().keySet()){
							targets = verificationMapping.getMap().get(source);
							double maxSim = 0.0;
							double sim;
				        	String maxTarget = "";
				        	int loopCnt = 0;
							for(String target: targets.keySet()){
								if(loopCnt == 0){
				        			maxTarget = target;
				        			maxSim = targets.get(target).doubleValue();
				        			loopCnt++;
				        		}
				        		sim = targets.get(target).doubleValue();
				        		 
				        		if(sim >= maxSim){
				        			maxSim = sim;
				        			maxTarget = target;
				        		}
							}
							if(maxSim >= thres){
								tp = new Tuple2<String,Double>(DatasetManager.removeDatasetFromResource(maxTarget),maxSim);
								localLinks.add(new Tuple2<String,Tuple2<String,Double>>(DatasetManager.removeDatasetFromResource(source),tp));
				        	}
						}
					   	return localLinks;
				}	
		};
		
		JavaPairRDD<String, String> links 
		= blocks.mapPartitionsToPair(linkF)
				.aggregateByKey(new HashSet<Tuple2<String,Double>>(), seqFunc, combFunc)
				.mapToPair(new PairFunction<Tuple2<String,Set<Tuple2<String,Double>>>,String,String>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, String> call(Tuple2<String, Set<Tuple2<String, Double>>> linkPair) 
					throws Exception {
						double maxSim = 0.0;
						String maxTarget = "";
						for(Tuple2<String,Double> link : linkPair._2){
							if(link._2 > maxSim){
								maxSim = link._2;
								maxTarget = link._1;
							}
						}
						return new Tuple2<String,String>(linkPair._1,maxTarget);
					}
				});
		return links;
	}
}

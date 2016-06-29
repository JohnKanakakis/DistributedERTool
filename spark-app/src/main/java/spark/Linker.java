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
import spark.model.DatasetManager;


public class Linker {
	
	public static final int CACHE_LIMIT = 10000; 
	public static Logger logger = LoggerFactory.getLogger(Linker.class);
	//public static final Accumulator<Integer> linkedResourcesCounter;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		List<Tuple2<String,Set<List<String>>>> ts = new ArrayList<Tuple2<String,Set<List<String>>>>();
        
        Set<List<String>> set = new HashSet<List<String>>(); 
        for(int i = 0; i < 100000; i++){
        	List<String> l = new ArrayList<String>();
        	for(int j = 0; j < 1000; j++){
        		l.add("e"+j);
        	}
        	set.add(l);
        	Tuple2<String, Set<List<String>>> t = new Tuple2<String,Set<List<String>>>("s"+i,set);
        	ts.add(t);
        }
		
		
		SparkConf sparkConf = new SparkConf().setAppName("Linker");
		Logger logger = sparkConf.log();
	
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	ctx.addJar("hdfs://master:8020/user/kanakakis/limes-core-1.0.0-SNAPSHOT.jar");

    	InputStream configFile = Utils.getHDFSFile(args[0]);
    	InputStream dtdFile = Utils.getHDFSFile(args[1]);
    	
    	XMLConfigurationReader reader = new XMLConfigurationReader();
		org.aksw.limes.core.io.config.Configuration config = null;
		config = reader.validateAndRead(configFile,dtdFile);
    	
		if(config == null){
			ctx.close();
			return;
		}
		
		Rewriter rw = RewriterFactory.getRewriter("Default");
        LinkSpecification ls = new LinkSpecification(config.getMetricExpression(), config.getVerificationThreshold());
        LinkSpecification rwLs = rw.rewrite(ls);
	    IPlanner planner = ExecutionPlannerFactory.getPlanner(config.getExecutionPlan(), null, null);
        assert planner != null;
        NestedPlan plan = planner.plan(rwLs);
        
        
        
        byte[] planBinary = Utils.serialize(plan);
        logger.info("config bytes = "+planBinary.length);
        
        String[] configParams = new String[2];
        configParams[0] = config.getSourceInfo().getVar();
        configParams[1] = config.getTargetInfo().getVar();
        
        double thres = config.getAcceptanceThreshold();
        
		Broadcast<byte[]> b = ctx.broadcast(planBinary);
		Broadcast<String[]> c = ctx.broadcast(configParams);
		Broadcast<Double> t = ctx.broadcast(thres);
		
		
		/*byte[] lsBinary = serialize(rwLs);
		
		Broadcast<byte[]> l = ctx.broadcast(lsBinary);
	
		logger.info("ls bytes = "+lsBinary.length);*/
		
		
		//serializeConfigurationToFile(config,configSerPath);
		//ctx.addFile(configSerPath.toString());
    	
    	//Broadcast<org.aksw.limes.core.io.config.Configuration> b_config = ctx.broadcast(config);
    	
    	
    	Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("textinputformat.record.delimiter", "\n");
        
        final JavaRDD<String> records = DataReader.run(ctx.newAPIHadoopFile(args[2], 
        											 TextInputFormat.class, 
        											 LongWritable.class, 
        											 Text.class,
        											 conf));
       
        JavaPairRDD<String, Set<List<String>>> blocks = ctx.parallelizePairs(ts,100);
        
		//JavaRDD<String> links = run(blocks,null,null,b,c,t);
		//links.saveAsTextFile(args[3]);
		ctx.close();
	}

	

	

	public static JavaPairRDD<String, Set<Tuple2<String, Double>>> run(JavaPairRDD<String, List<List<String>>> blocks, 
			               			  final Broadcast<byte[]> skb,
			               			  final Broadcast<byte[]> tkb,
									  final Broadcast<byte[]> p, 
									  final Broadcast<String[]> c,
									  final Broadcast<Double> thres, 
									  final Integer optimalBlockSize) {
		
		final KBInfo sourceKb = (KBInfo) Utils.deserialize(skb.value());
		final KBInfo targetKb = (KBInfo) Utils.deserialize(tkb.value());
		final NestedPlan plan = (NestedPlan) Utils.deserialize(p.getValue());
		final String[] configParams = c.getValue();
		final String sourceVar = configParams[0];
		final String targetVar = configParams[1];
		
		
		
		Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>> seqFunc = 
				new Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> personInfoSetPerKey, 
							Tuple2<String,Double> personInfo) 
					throws Exception {
						// TODO Auto-generated method stub
						personInfoSetPerKey.add(personInfo);
						return personInfoSetPerKey;
					}
		};
		
		Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>> combFunc = 
				new Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> personInfoSetPerKey_1, 
											Set<Tuple2<String,Double>> personInfoSetPerKey_2) 
					throws Exception {
						// TODO Auto-generated method stub
						personInfoSetPerKey_1.addAll(personInfoSetPerKey_2);
						return personInfoSetPerKey_1;
					}
		};
		
		
		PairFlatMapFunction<Tuple2<String, List<List<String>>>, String, Tuple2<String, Double>> linkF =
				new PairFlatMapFunction<Tuple2<String, List<List<String>>>, String, Tuple2<String,Double>>(){
					private static final long serialVersionUID = 1L;
					SimpleExecutionEngine engine = new SimpleExecutionEngine();
					
					@Override
					public Iterable<Tuple2<String, Tuple2<String,Double>>> call(Tuple2<String, List<List<String>>> blockPair) throws Exception {
						// TODO Auto-generated method stub
						ArrayList<Tuple2<String,Tuple2<String,Double>>> partitionLinks = 
								new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
						
						
						
						MemoryCache sourceCache = new MemoryCache();
					    MemoryCache targetCache = new MemoryCache();
					    MemoryCache cache = null;
					    //ArrayList<Tuple2<String,Tuple2<String,Double>>> localLinks = null;
						
						KBInfo kb = null;
					    
						
						
						List<List<String>> block;
						//List<String> resourceInfo = null;
					    String subject = null;
						String predicate = null;
						String object = null;
						String value;
						String datasetId;
						
						int cnt1 = 0;
						int cnt2 = 0;
						int cnt = 0;
						//boolean limit_reached = false;
						//Tuple2<String, Set<List<String>>> t;
						//while(blocksPerPartition.hasNext()){
							//t = blocksPerPartition.next();
							block = blockPair._2;
							//
							
							//logger.info("("+t._1+","+block.size()+")");
							
							if(block.size() < 2) return partitionLinks;
							if(block.size() > optimalBlockSize){
								logger.info("mega block size = "+block.size()+ "for token = "+blockPair._1);
								return partitionLinks;
							}
							
							//cnt++;
							//Iterator<List<String>> it = block._2.iterator();
							//sourceCache.clear();
							//targetCache.clear();
							//boolean limit_reached = false;
						   	for(List<String> resourceInfo : block){
						   		//if(limit_reached) break;
						    	subject = resourceInfo.get(0);
						    	datasetId = DatasetManager.getDatasetIdOfResource(subject);
						    	if(datasetId.equals(sourceKb.getId())){
						    		cache = sourceCache;
						    		kb = sourceKb;
						    		//cnt1++;
						    	}else if(datasetId.equals(targetKb.getId())){
						    		cache = targetCache;
						    		kb = targetKb;
						    		//cnt2++;
						    	}
						    	/*if( (cnt1 > CACHE_LIMIT) || (cnt2 > CACHE_LIMIT)){
						    		limit_reached = true;
						    		continue;
						    	}*/
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
											//System.out.println("examing property: "+predicate);
											//remove localization information, e.g. @en
											for (String propertyDub : kb.getFunctions().get(predicate).keySet()) {
												
												//System.out.println("raw value is "+rawValue);
												value = Preprocessor.process(object, kb.getFunctions().get(predicate).get(propertyDub));
												
												//System.out.println("adding statement "+ predicate +" "+ propertyDub +" "+value);
												cache.addTriple(subject, propertyDub, value);
											}
											//System.out.println("----------------------------------------------------");
										}
										
									}
									/*else{
										//System.out.println("adding statement "+ st.toString());
										cache.addTriple(subject, predicate, object);
									}*/
						    	}
						    }
						   	if(sourceCache.size() == 0 || targetCache.size() == 0) return partitionLinks;
						   	
						   	partitionLinks = this.link(sourceCache, targetCache,partitionLinks);
						   	
						   	return partitionLinks;
					}		
					
					private ArrayList<Tuple2<String,Tuple2<String,Double>>> link(MemoryCache s,
							 												     MemoryCache t,
					 												     		 ArrayList<Tuple2<String,Tuple2<String,Double>>> links){


							engine.configure(s, t, sourceVar, targetVar);

							Mapping verificationMapping = engine.execute(plan);

							Tuple2<String,Double> tp = null;
							String sourceInfo;
							String targetInfo;
							//logger.info("number of links: "+acceptanceMapping.getMap().keySet().size());
							logger.info("source cache:"+s.size());
							
							HashMap<String, Double> targets;
							double sim;
							int linksCnt = 0;
							for(String source: verificationMapping.getMap().keySet()){
								sourceInfo = s.getInstance(source).toString();
								targets = verificationMapping.getMap().get(source);
								links.ensureCapacity(targets.keySet().size());
								for(String target: targets.keySet()){
									sim = targets.get(target);
									if(sim >= thres.getValue()){
										linksCnt++;
										targetInfo = t.getInstance(target).toString();
										tp = new Tuple2<String,Double>(targetInfo,sim);
										links.add(new Tuple2<String,Tuple2<String,Double>>(sourceInfo,tp));
									}
								}
							}
							logger.info("number of links "+linksCnt);
							return links;
					}
		};
		
		JavaPairRDD<String, Set<Tuple2<String, Double>>> links = blocks.flatMapToPair(linkF)
									  .aggregateByKey(new HashSet<Tuple2<String,Double>>(), seqFunc, combFunc);
									  /*.flatMap(new FlatMapFunction<Tuple2<String,Set<Tuple2<String,Double>>>,String>(){
											private static final long serialVersionUID = 1L;
											@Override
											public Iterable<String> call(Tuple2<String, Set<Tuple2<String,Double>>> linkPairs) throws Exception {
												// TODO Auto-generated method stub
												String s = linkPairs._1;
												ArrayList<String> links = new ArrayList<String>();
												//links.add(s+" "+linkPairs._2);
												for(Tuple2<String,Double> t:linkPairs._2){
													
													links.add(s+" "+OWL.sameAs.getURI() + " "+t);
												}
												return links;
											}
								  		});*/
		return links;
	
	}

	
	public static JavaPairRDD<String, Set<Tuple2<String, Double>>> runWithList(JavaPairRDD<String, List<String>> blocks, 
																 			  final Broadcast<byte[]> skb,
																 			  final Broadcast<byte[]> tkb,
																			  final Broadcast<byte[]> p, 
																			  final Broadcast<String[]> c,
																			  final Broadcast<Double> thres, 
																			  final Integer optimalBlockSize) {

		final KBInfo sourceKb = (KBInfo) Utils.deserialize(skb.value());
		final KBInfo targetKb = (KBInfo) Utils.deserialize(tkb.value());
		final NestedPlan plan = (NestedPlan) Utils.deserialize(p.getValue());
		final String[] configParams = c.getValue();
		final String sourceVar = configParams[0];
		final String targetVar = configParams[1];



		Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>> seqFunc = 
		new Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> personInfoSetPerKey, 
				Tuple2<String,Double> personInfo) 
			throws Exception {
			// TODO Auto-generated method stub
				personInfoSetPerKey.add(personInfo);
				return personInfoSetPerKey;
			}
		};

		Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>> combFunc = 
		new Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> personInfoSetPerKey_1, 
								Set<Tuple2<String,Double>> personInfoSetPerKey_2) 
			throws Exception {
			// TODO Auto-generated method stub
				personInfoSetPerKey_1.addAll(personInfoSetPerKey_2);
				return personInfoSetPerKey_1;
			}
		};


		PairFlatMapFunction<Tuple2<String, List<String>>, String, Tuple2<String, Double>> linkF =
		new PairFlatMapFunction<Tuple2<String, List<String>>, String, Tuple2<String,Double>>(){
		private static final long serialVersionUID = 1L;
			SimpleExecutionEngine engine = new SimpleExecutionEngine();
			
			@Override
			public Iterable<Tuple2<String, Tuple2<String,Double>>> call(Tuple2<String, List<String>> blockPair) throws Exception {
			// TODO Auto-generated method stub
			ArrayList<Tuple2<String,Tuple2<String,Double>>> links = 
					new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
			
			
			int cnt1 = 0;
			int cnt2 = 0;
			int cnt = 0;
			String subject = null;
			String predicate = null;
			String object = null;
			String value;
			String datasetId;
			
			for(int i = 0; i < blockPair._2.size()-1; i++){
				if(blockPair._2.get(i).equals("@@@")){
					cnt++;
					subject = blockPair._2.get(i+1);
				  	datasetId = DatasetManager.getDatasetIdOfResource(subject);
				  	if(datasetId.equals(sourceKb.getId())){
				  		cnt1++;
				  	}else if(datasetId.equals(targetKb.getId())){
				  		cnt2++;
				  	}
				}
			}
			
			
			if(cnt == 0 || cnt1 == 0 || cnt2 == 0){
				return links;
			}
			
			if(cnt > optimalBlockSize)
				return links;
			
			
			MemoryCache sourceCache = new MemoryCache(cnt1);
			MemoryCache targetCache = new MemoryCache(cnt2);
			MemoryCache cache = null;
			KBInfo kb = null;
			
			
			for(int i = 0; i < blockPair._2.size()-2; i++){
				if(blockPair._2.get(i).equals("@@@")){
					cnt++;
					subject = blockPair._2.get(i+1);
				  	datasetId = DatasetManager.getDatasetIdOfResource(subject);
				  	if(datasetId.equals(sourceKb.getId())){
				  		cache = sourceCache;
				  		kb = sourceKb;
				  	}else if(datasetId.equals(targetKb.getId())){
				  		cache = targetCache;
				  		kb = targetKb;
				  	}
				  	int j = i+2;
				  	while(j < blockPair._2.size() && !blockPair._2.get(j).equals("@@@")){
				  		predicate = blockPair._2.get(j);
				  		
				  		if(kb.getProperties().contains(predicate)){
				  			object = DataFormatter.eliminateDataTypeFromLiteral(blockPair._2.get(j+1));
				    		if(kb.getFunctions().get(predicate).keySet().size() == 0){
								cache.addTriple(subject, predicate, object);
				    		}
							else{
								for(String propertyDub : kb.getFunctions().get(predicate).keySet()) {
									value = Preprocessor.process(object, kb.getFunctions().get(predicate).get(propertyDub));
									cache.addTriple(subject, propertyDub, value);
								}
							}	
						}	
				  		j = j + 2;
				  	}
				}
			}
					 	
		 	links = this.link(sourceCache, targetCache,links);
		 	
		 	return links;
		}		

		private ArrayList<Tuple2<String,Tuple2<String,Double>>> link(MemoryCache s,
		 												     MemoryCache t,
												     		 ArrayList<Tuple2<String,Tuple2<String,Double>>> links){
	
		
			engine.configure(s, t, sourceVar, targetVar);
		
			Mapping verificationMapping = engine.execute(plan);
		
			Tuple2<String,Double> tp = null;
			String sourceInfo;
			String targetInfo;
			//logger.info("number of links: "+acceptanceMapping.getMap().keySet().size());
			logger.info("source cache:"+s.size());
			logger.info("target cache:"+t.size());
			
			HashMap<String, Double> targets;
			double sim;
			int linksCnt = 0;
			for(String source: verificationMapping.getMap().keySet()){
				sourceInfo = s.getInstance(source).toString();
				targets = verificationMapping.getMap().get(source);
				links.ensureCapacity(targets.keySet().size());
				for(String target: targets.keySet()){
					sim = targets.get(target);
					if(sim >= thres.getValue()){
						linksCnt++;
						targetInfo = t.getInstance(target).toString();
						tp = new Tuple2<String,Double>(targetInfo,sim);
						links.add(new Tuple2<String,Tuple2<String,Double>>(sourceInfo,tp));
					}
				}
			}
			logger.info("number of links "+linksCnt);
			return links;
		}
};

JavaPairRDD<String, Set<Tuple2<String, Double>>> links = blocks.flatMapToPair(linkF)
			  .aggregateByKey(new HashSet<Tuple2<String,Double>>(), seqFunc, combFunc);
			  /*.flatMap(new FlatMapFunction<Tuple2<String,Set<Tuple2<String,Double>>>,String>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Iterable<String> call(Tuple2<String, Set<Tuple2<String,Double>>> linkPairs) throws Exception {
						// TODO Auto-generated method stub
						String s = linkPairs._1;
						ArrayList<String> links = new ArrayList<String>();
						//links.add(s+" "+linkPairs._2);
						for(Tuple2<String,Double> t:linkPairs._2){
							
							links.add(s+" "+OWL.sameAs.getURI() + " "+t);
						}
						return links;
					}
		  		});*/
return links;

}
	
	
}

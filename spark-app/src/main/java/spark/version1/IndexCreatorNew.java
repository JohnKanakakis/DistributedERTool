package spark.version1;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.preprocessing.Preprocessor;
import org.apache.commons.codec.language.Soundex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashBigSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import scala.Tuple2;
import spark.Utils;
import spark.blocking.BlockCreator;
import spark.filter.DataFormatter;
import spark.io.DataReader;
import spark.io.InstanceIndexReader;
import spark.model.DatasetInfo;
import spark.model.DatasetManager;

public class IndexCreatorNew {

	public static String[] commonTokens = {"a","and","it","of"," "};
	
	public static Logger logger = LoggerFactory.getLogger(IndexCreatorNew.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("Controller");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
		Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("textinputformat.record.delimiter", ")\n");
      
        JavaRDD<String> indexData = DataReader.run(ctx.newAPIHadoopFile(args[0], 
														 TextInputFormat.class, 
														 LongWritable.class, 
														 Text.class,
														 conf));
    	
       
        
        JavaPairRDD<String, List<String>> personIndex = InstanceIndexReader.run(indexData);
        /*
        List<Tuple2<String, String>> sample = personIndex.take(10);
        for(int i = 0; i < sample.size(); i++){
        	sparkConf.log().info(sample.get(i)._1+"|"+sample.get(i)._2);
        }*/
        
       // JavaPairRDD<String, Tuple2<String, String>> blocks = run(personIndex,null,null);
       
    	//blocks.saveAsTextFile(args[1]);
        
        ctx.close();
	}

	
	
	
	public static JavaPairRDD<String, List<String>> createBlocks(JavaPairRDD<String,List<String>> resources,
																  final Broadcast<byte[]> skbB,
																  final Broadcast<byte[]> tkbB)
	{
		final KBInfo skb = (KBInfo)Utils.deserialize(skbB.getValue());
		final KBInfo tkb = (KBInfo)Utils.deserialize(tkbB.getValue());
		final Set<String> sourceProperties = new HashSet<String>(skb.getProperties());
		final Set<String> targetProperties = new HashSet<String>(tkb.getProperties());
		
		PairFlatMapFunction<Tuple2<String, List<String>>, String, String> f = 
		
			
		new PairFlatMapFunction<Tuple2<String,List<String>>,String,String>(){
		
			private static final long serialVersionUID = 1L;
			
			@Override
			public Iterable<Tuple2<String,String>> call(Tuple2<String, List<String>> resource)
			throws Exception {
			// TODO Auto-generated method stub

				HashSet<Tuple2<String,String>> tokenPairs = 
						new HashSet<Tuple2<String,String>>();
				
				Set<String> kbProperties = null;
				//Tuple2<String, List<String>> resource = null;
				String r_id = null;
				List<String> r_info = null;
				KBInfo kb = null;
				String predicate = null;
				String object = null;
				String value = null;
				String valueToIndex = "";
				//String BKV = new String();
				String bkv = null;
				Tuple2<String, String> t;
				String datasetId = "";
				String[] tokens;
				
				//while(resources.hasNext()){
					
					//resource = resources.next();
					r_id = resource._1;
					r_info = resource._2;
					datasetId = DatasetManager.getDatasetIdOfResource(r_id);
					if(datasetId.equals(tkb.getId())){
						kb = tkb;
						kbProperties = targetProperties;
						
					}else if(datasetId.equals(skb.getId())){
						
						kb = skb;
						kbProperties = sourceProperties;
					}
					bkv = "";
					for(int i = 0; i < r_info.size()-1; i = i + 2){
						predicate = r_info.get(i);
						object = DataFormatter.eliminateDataTypeFromLiteral(r_info.get(i+1));
						object = object.replace("\"", "");
						if(kbProperties.contains(predicate)){
							object = DataFormatter.eliminateDataTypeFromLiteral(object);
							bkv+=(object.replace("\"", "")+" ");
							
						}
					}
					
					tokens = bkv.split(" ");
					
					//logger.info("tokens:"+tokens);
					r_info.add(0, "@@@");
					r_info.add(1, r_id);
					for(int i = 0; i < tokens.length; i++){
						tokens[i] = tokens[i].trim().replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
						//logger.info(tokens[i]);
						if(belongsToCommonTokens(tokens[i])) continue;
						
						t = new Tuple2<String,String>(tokens[i],r_id);
						
						tokenPairs.add(t);
					}
					
				//}
				return tokenPairs;	
			}
			
			
		};

		
		/*Function2<List<List<String>>,List<String>,List<List<String>>> seqFunc = 
				new Function2<List<List<String>>,List<String>,List<List<String>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public List<List<String>> call(List<List<String>> resourcesPerKey, 
							List<String> resource) 
					throws Exception {
						// TODO Auto-generated method stub
						resourcesPerKey.add(resource);
						return resourcesPerKey;
					}
		};
		
		Function2<List<List<String>>,List<List<String>>,List<List<String>>> combFunc = 
				new Function2<List<List<String>>,List<List<String>>,List<List<String>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public List<List<String>> call(List<List<String>> resourcesPerKey_1, 
							List<List<String>> resourcesPerKey_2) 
					throws Exception {
						// TODO Auto-generated method stub
						resourcesPerKey_1.addAll(resourcesPerKey_2);
						return resourcesPerKey_1;
					}
		};*/
		
		Function2<List<String>, List<String>, List<String>> reduceF = 
				new Function2<List<String>, List<String>, List<String>>(){
					private static final long serialVersionUID = 1L;

					@Override
					public List<String> call(List<String> v1, List<String> v2) throws Exception {
						// TODO Auto-generated method stub
						v1.addAll(v2);
						return v1;
					}
			
		};
		
		Function<Tuple2<String,List<String>>,Boolean> purge = new Function<Tuple2<String,List<String>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, List<String>> block) throws Exception {
				// TODO Auto-generated method stub
				int cnt1 = 0;
				int cnt2 = 0;
				int cnt = 0;
				String subject = null;
				
				String datasetId;
				
				for(int i = 0; i < block._2.size()-1; i++){
					if(block._2.get(i).equals("@@@")){
						cnt++;
						subject = block._2.get(i+1);
					  	datasetId = DatasetManager.getDatasetIdOfResource(subject);
					  	if(datasetId.equals(skb.getId())){
					  		cnt1++;
					  	}else if(datasetId.equals(tkb.getId())){
					  		cnt2++;
					  	}
					}
				}
				
				
				if(cnt == 0 || cnt1 == 0 || cnt2 == 0){
					return false;
				}
				
				if(cnt > 20000)
					return false;
				
				return true;
			}
			
		};
		Function2<List<String>, String, List<String>> seqF = 
				new Function2<List<String>, String, List<String>>(){
					private static final long serialVersionUID = 1L;

					@Override
					public List<String> call(List<String> l, String s) throws Exception {
						// TODO Auto-generated method stub
						l.add(s);
						return l;
					}
			
		};
		
		//HashPartitioner hp = new HashPartitioner(1000);
		JavaPairRDD<String, List<String>> result = resources.flatMapToPair(f)
					  									    .aggregateByKey(new ArrayList<String>(),seqF,reduceF);
					  									    //.filter(purge);
			 
		
		return result;
	}
	
	protected static boolean belongsToCommonTokens(String token) {
		// TODO Auto-generated method stub
		for(int i = 0; i < commonTokens.length;i++)
			if(commonTokens[i].equals(token))
				return true;
		return false;
	}


	private static String encryptedHashValue(String value){
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		messageDigest.update(value.getBytes());
		return new String(messageDigest.digest());
	}
	
	private static String soundEncoded(String value){
		value = value.replaceAll("\\p{Digit}|\\p{Punct}", "");
		Soundex soundex = new Soundex();
		String result = null;
		try{
			result = soundex.encode(value);
			return result;
		}catch(IllegalArgumentException e){
			//logger.error("value is "+value);
			return null;
		}
		
	}


	public static JavaPairRDD<Integer, Integer> getFrequencyOfBlocks(JavaPairRDD<String, List<String>> blocks) {
		// TODO Auto-generated method stub
		return
				//(block_key,N) => (N,1)
		        blocks.mapToPair(new PairFunction<Tuple2<String,List<String>>,Integer,Integer>(){

					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<Integer, Integer> call(Tuple2<String, List<String>> t) throws Exception {
						// TODO Auto-generated method stub
						int cnt = 0;
						for(int i = 0; i < t._2.size(); i++){
							if(t._2.get(i).equals("@@@"))
								cnt++;
						}
						return new Tuple2<Integer,Integer>(cnt,1);
					}
		    	})
		        
		        //(N,freq)
		        .foldByKey(0, new Function2<Integer,Integer,Integer>(){
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
		    	});
				
	}


	
}



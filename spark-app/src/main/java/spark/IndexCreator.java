package spark;

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
import spark.blocking.BlockCreator;
import spark.filter.DataFormatter;
import spark.io.DataReader;
import spark.io.InstanceIndexReader;
import spark.model.DatasetInfo;
import spark.model.DatasetManager;

public class IndexCreator {

	public static String[] commonTokens = {"a","and","it","of"," "};
	
	public static Logger logger = LoggerFactory.getLogger(IndexCreator.class);
	
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

	
	/*public static JavaPairRDD<String, Tuple2<String, String>> run(JavaPairRDD<String,List<String>> index,
																  final Broadcast<byte[]> kbB)
	{
		final KBInfo kb = (KBInfo)Utils.deserialize(kbB.getValue());
		PairFlatMapFunction<Tuple2<String, List<String>>, String, Tuple2<String, String>> f = 
				new PairFlatMapFunction<Tuple2<String,List<String>>,String,Tuple2<String,String>>(){

					private static final long serialVersionUID = 1L;
		
					@Override
					public Iterable<Tuple2<String,Tuple2<String,String>>> call(Tuple2<String, List<String>> resource)
					throws Exception {
						// TODO Auto-generated method stub

						String r_id = resource._1;
						List<String> r_info = resource._2;
						
						ArrayList<Tuple2<String,Tuple2<String,String>>> tokenPairs = 
								new ArrayList<Tuple2<String,Tuple2<String,String>>>();
						
						String predicate = null;
						String object = null;
						Tuple2<String,String> t;
						for(int i = 0; i < r_info.size()-1; i = i + 2){
							predicate = r_info.get(i);
							object = DataFormatter.eliminateDataTypeFromLiteral(r_info.get(i+1));
							if(kb.getProperties().contains(predicate)){
								//System.out.println("examing property: "+predicate);
								//remove localization information, e.g. @en
								for (String propertyDub : kb.getFunctions().get(predicate).keySet()) {
									object = Preprocessor.process(object, kb.getFunctions().get(predicate).get(propertyDub));
									t = new Tuple2<String,String>(soundEncoded(object),r_id);
									tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(r_id,t));
								}
							}
							
							else{
								t = new Tuple2<String,String>(soundEncoded(object),r_id);
								tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(r_id,t));
							}
							
							
							if(!r_info.get(i).equals(ResourceIndexCreator.TYPE_PROPERTY.toString())){
								object = r_info.get(i+1);
								object = DataFormatter.eliminateDataTypeFromLiteral(object);
								t = new Tuple2<String,String>(object,r_id);
								tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(r_id,t));
							}							
						}
						
						return tokenPairs;	
					}
		};
		return index.flatMapToPair(f);
	}*/
	
	public static JavaPairRDD<String, Tuple2<String, String>> run(JavaPairRDD<String,List<String>> index,
																  final Broadcast<byte[]> skbB,
																  final Broadcast<byte[]> tkbB)
	{
		final KBInfo skb = (KBInfo)Utils.deserialize(skbB.getValue());
		final KBInfo tkb = (KBInfo)Utils.deserialize(tkbB.getValue());
		final Set<String> sourceProperties = new HashSet<String>(skb.getProperties());
		final Set<String> targetProperties = new HashSet<String>(tkb.getProperties());
		
		PairFlatMapFunction<Iterator<Tuple2<String, List<String>>>, String, Tuple2<String,String>> f = 
		
		// pair = (BKV, (valueOfProperty, r_id) )		
		new PairFlatMapFunction<Iterator<Tuple2<String,List<String>>>,String,Tuple2<String,String>>(){
		
			private static final long serialVersionUID = 1L;
			//private HashMap<String,Set<Tuple2<String,String>>> index = new HashMap<String,Set<Tuple2<String,String>>>();
			@Override
			public ObjectOpenHashBigSet<Tuple2<String, Tuple2<String, String>>> call(Iterator<Tuple2<String, List<String>>> resources)
			throws Exception {
			// TODO Auto-generated method stub

				Set<Tuple2<String, Set<Tuple2<String, String>>>> indexAsSet =
						new HashSet<Tuple2<String, Set<Tuple2<String, String>>>>();
				
				ObjectOpenHashBigSet<Tuple2<String,Tuple2<String,String>>> tokenPairs = 
						new ObjectOpenHashBigSet<Tuple2<String,Tuple2<String,String>>>();
				
				Set<String> kbProperties = null;
				Tuple2<String, List<String>> resource = null;
				String r_id = null;
				List<String> r_info = null;
				KBInfo kb = null;
				String predicate = null;
				String object = null;
				String value = null;
				String valueToIndex = "";
				//String BKV = new String();
				String bkv = null;
				Tuple2<String,String> t;
				String datasetId = "";
				String[] tokens;
				
				while(resources.hasNext()){
					
					resource = resources.next();
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
							//remove localization information, e.g. @en
							bkv+=(object.replace("\"", "")+" ");
							
							/*if(kb.getFunctions().get(predicate).keySet().size() == 0){
								//valueToIndex+=(object.replace("\"", "")+" ");
								
								valueToIndex=(object.replace("\"", "")+" ");
								bkv = valueToIndex;
								t = new Tuple2<String,String>(valueToIndex,r_id);
								tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(bkv,t));
								
								bkv = soundEncoded(valueToIndex);
								
								if(bkv != null && !bkv.equals("")){
									t = new Tuple2<String,String>(valueToIndex,r_id);
									//addPairToIndex(BKV,t);
									tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(bkv,t));
								}else{
									logger.error(r_id + " "+r_info + "\n is not valid resource for encode");
									continue;
								}
							}
							
							for (String propertyDub : kb.getFunctions().get(predicate).keySet()) {
								value = Preprocessor.process(object, kb.getFunctions().get(predicate).get(propertyDub));
								//valueToIndex+=(value.replace("\"", "")+" ");
								bkv = soundEncoded(valueToIndex);
								
								if(bkv != null && !bkv.equals("")){
									t = new Tuple2<String,String>(valueToIndex,r_id);
									//addPairToIndex(BKV,t);
									tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(bkv,t));
								}else{
									logger.error(r_id + " "+r_info + "\n is not valid resource for encode");
									continue;
								}
								valueToIndex =(value.replace("\"", "")+" ");
								bkv = valueToIndex;
								t = new Tuple2<String,String>(valueToIndex,r_id);
								tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(bkv,t));
							}*/
						}
					}
					/*if(kbProperties.equals(targetProperties)){
						logger.info("target value to index "+valueToIndex);
					}else{
						logger.info("source value to index "+valueToIndex);
					}*/
					/*if(valueToIndex.equals("")){
						logger.error(r_id + " "+r_info + "\n is not valid resource");
						continue;
					}*/
					//bkv = soundEncoded(valueToIndex);
					/*bkv = valueToIndex;
					if(bkv != null && !bkv.equals("")){
						t = new Tuple2<String,String>(valueToIndex,r_id);
						//addPairToIndex(BKV,t);
						tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(bkv,t));
					}else{
						//logger.error(r_id + " "+r_info + "\n is not valid resource for encode");
						continue;
					}*/
					//logger.info("BKV:"+bkv);
					tokens = bkv.split(" ");
					
					//logger.info("tokens:"+tokens);
					
					for(int i = 0; i < tokens.length; i++){
						//if(tokens[i].length() > 3){
							tokens[i] = tokens[i].trim().replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
							//logger.info(tokens[i]);
							if(belongsToCommonTokens(tokens[i])) continue;
							t = new Tuple2<String,String>(tokens[i],r_id);
							//tokenPairs.add(t);
							tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(r_id,t));
						//}
					}
					
				}
				//indexAsSet = Utils.toSet(index);
				return tokenPairs;	
			}
			
			/*private void addPairToIndex(String BKV, Tuple2<String, String> t) {
				// TODO Auto-generated method stub
				Set<Tuple2<String, String>> set;
				if(index.containsKey(BKV)){
					set = index.get(BKV);
				}else{
					set = new HashSet<Tuple2<String, String>>();
				}
				set.add(t);
				index.put(BKV, set);
			}*/
		};

		
		/*Function2<HashSet<Tuple2<String, String>>, 
				  HashSet<Tuple2<String, String>>, 
				  HashSet<Tuple2<String, String>>> combFunc = 
		
		new Function2<HashSet<Tuple2<String, String>>, 
					  HashSet<Tuple2<String, String>>, 
					  HashSet<Tuple2<String, String>>>(){

						private static final long serialVersionUID = 1L;

						@Override
						public HashSet<Tuple2<String, String>> call(HashSet<Tuple2<String, String>> set1,
								HashSet<Tuple2<String, String>> set2) throws Exception {
							// TODO Auto-generated method stub
							set1.addAll(set2);
							return set1;
						}
			
		};
		
		Function2<HashSet<Tuple2<String, String>>, 
		  				  Tuple2<String, String>, 
		  				  HashSet<Tuple2<String, String>>> seqFunc = 

	    new Function2<HashSet<Tuple2<String, String>>, 
			  		  Tuple2<String, String>, 
			  		  HashSet<Tuple2<String, String>>>(){

						private static final long serialVersionUID = 1L;
		
						@Override
						public HashSet<Tuple2<String, String>> call(HashSet<Tuple2<String, String>> set,
								  Tuple2<String, String> t) throws Exception {
							// TODO Auto-generated method stub
							set.add(t);
							return set;
						}
	
		};
		
		PairFlatMapFunction<Tuple2<String, HashSet<Tuple2<String, String>>>, String, Tuple2<String, String>> sortedNeighborBlocking = 
				new PairFlatMapFunction<Tuple2<String, HashSet<Tuple2<String, String>>>, String,Tuple2<String,String>>(){
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String,Tuple2<String,String>>> call(Tuple2<String, HashSet<Tuple2<String, String>>> indexBlock) throws Exception {
						// TODO Auto-generated method stub
						String BKV = indexBlock._1;
						List<Tuple2<String,String>> l = new ArrayList<Tuple2<String,String>>(indexBlock._2);
						Comparator<Tuple2<String,String>> c = new Tuple2Comparator();
						Collections.sort(l, c);
						
						int L = l.size();
						int k = L%W;
						
						Tuple2<String,String> indexBlockPair = null;
						Tuple2<String,Tuple2<String,String>> blockTuple = null;
						String r_id = null;
						List<Tuple2<String,Tuple2<String,String>>> result = new ArrayList<Tuple2<String,Tuple2<String,String>>>();
						for(int j = 0; j < k; j++){
							for(int i = j*W; i < (j+1)*W; i++){
								indexBlockPair = l.get(i);
								r_id = indexBlockPair._2;
								blockTuple = new Tuple2<String,Tuple2<String,String>>(r_id,
																					  new Tuple2<String,String>(BKV+"_"+i,r_id));
								result.add(blockTuple);
							}
						}
						return result;
					}
			
		};*/
		
		//ObjectOpenHashBigSet<String> zeroValue = new ObjectOpenHashBigSet<String>();
		
		JavaPairRDD<String, Tuple2<String, String>> result = index.mapPartitionsToPair(f);
					  													  /* .aggregateByKey(zeroValue, 
					  															   		   Utils.getSeqFunction(), 
				  															   		   	   Utils.getCombFunc());*/
			 
		
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


	public static JavaPairRDD<Integer, Integer> getFrequencyOfBlocks(JavaPairRDD<String, Tuple2<String, String>> resourceIndex) {
		// TODO Auto-generated method stub
		return
				//(block_key,1)
		        resourceIndex.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>,String,Integer>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String,Integer> call(Tuple2<String, Tuple2<String, String>> indexPair) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String,Integer>(indexPair._2._1,1);
					}
		    	})
		        //(block_key,N)
		        .foldByKey(0, new Function2<Integer,Integer,Integer>(){
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
		    	})
		        //(N,1)
		        .mapToPair(new PairFunction<Tuple2<String,Integer>,Integer,Integer>(){
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> call(Tuple2<String, Integer> t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Integer,Integer>(t._2,1);
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



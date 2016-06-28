package spark.blocking;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.DatabaseConfiguration;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import scala.Tuple2;

public class BlockCreator {

	public static Logger logger = LoggerFactory.getLogger(BlockCreator.class);//Logger.getLogger(BlockCreator.class);
	
	
	//blocks = (p_id, (w, p_id) )
	public static JavaPairRDD<String, Tuple2<String, String>> runWithTwoProperties(JavaPairRDD<String, List<String>> index, 
																				  final Accumulator<Integer> blockedEntities, 
																				  final Accumulator<Integer> unblockedEntities){
		
		//Partitioner indexPartitioner = index.partitioner().get();
		
		/*JavaPairRDD<String, Tuple2<String, String>> result = index.flatMapToPair(
	    		
		    	new PairFlatMapFunction<Tuple2<String,List<String>>,String,String>(){

				private static final long serialVersionUID = 1L;

				@Override
				public Iterable<Tuple2<String,String>> call(Tuple2<String, List<String>> instancePair) throws Exception {
					// TODO Auto-generated method stub
					String p_id = instancePair._1;
					ArrayList<Tuple2<String,String>> pairs = 
							new ArrayList<Tuple2<String,String>>();
					
					String[] predicates = new String[3];
					
					predicates[0] = "http://xmlns.com/foaf/0.1/firstName";//"foaf:firstName";
					predicates[1] = "http://xmlns.com/foaf/0.1/lastName";//"foaf:lastName";
					//predicates[2] = "#foaf:name";
					
					String key = null;
					Tuple2<String,String> t;
					for(int i = 0; i < predicates.length-1;i++){
						key = getPredicateObjectFromNT(instancePair._2,predicates[i]);
						if(key != null){
							t = new Tuple2<String,String>(key,p_id);
							pairs.add(t);
							blockedEntities.add(1);
						}else{
							unblockedEntities.add(1);
						}
					}
					if(pairs.size() == 0){
						key = getPredicateValue(instancePair._2,predicates[predicates.length-1]);
						if(key != null){
							pairs.add(new Tuple2<String,String>(key,instancePair._1));
						}else{
							pairs.add(new Tuple2<String,String>("",""));
						//}
					}
					return pairs;	
				}

		    }).keyBy(new Function<Tuple2<String,String>,String>(){

				private static final long serialVersionUID = 1L;

				@Override
				public String call(Tuple2<String, String> token_resourceId_pair) throws Exception {
					// TODO Auto-generated method stub
					return token_resourceId_pair._2;
				}
		    });*/
		JavaPairRDD<String, Tuple2<String, String>> result = 
				index.flatMapToPair(new PairFlatMapFunction<Tuple2<String,List<String>>,String,Tuple2<String,String>>(){

				private static final long serialVersionUID = 1L;

				@Override
				public Iterable<Tuple2<String,Tuple2<String,String>>> call(Tuple2<String, List<String>> instancePair)
				throws Exception {
					// TODO Auto-generated method stub
					String p_id = instancePair._1;
					ArrayList<Tuple2<String,Tuple2<String,String>>> pairs = 
							new ArrayList<Tuple2<String,Tuple2<String,String>>>();
					
					String[] predicates = new String[3];
					
					predicates[0] = "http://xmlns.com/foaf/0.1/firstName";//"foaf:firstName";
					predicates[1] = "http://xmlns.com/foaf/0.1/lastName";//"foaf:lastName";
					//predicates[2] = "#foaf:name";
					
					String key = null;
					Tuple2<String,String> t;
					for(int i = 0; i < predicates.length-1;i++){
						key = getPredicateObjectFromNT(instancePair._2,predicates[i]);
						if(key != null){
							t = new Tuple2<String,String>(key,p_id);
							pairs.add(new Tuple2<String,Tuple2<String,String>>(p_id,t));
							blockedEntities.add(1);
						}else{
							unblockedEntities.add(1);
						}
					}
					/*if(pairs.size() == 0){
						key = getPredicateValue(instancePair._2,predicates[predicates.length-1]);
						if(key != null){
							pairs.add(new Tuple2<String,String>(key,instancePair._1));
						}else{
							pairs.add(new Tuple2<String,String>("",""));
						//}
					}*/
					return pairs;	
				}
			});/*.mapToPair(new PairFunction<Tuple2<String,String>,String,Tuple2<String,String>>(){
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> pair) throws Exception {
					// TODO Auto-generated method stub
					String personId = pair._2;
					return new Tuple2<String,Tuple2<String,String>>(personId,pair);
				}
		    	
		    });*/
		    return result;
	}
	
/*	public static JavaPairRDD<String, Tuple2<String, String>> runSetWithTwoProperties(
			JavaPairRDD<String, Set<String>> personIndex, 
			final Accumulator<Integer> blockedEntities,
			final Accumulator<Integer> unblockedEntities) {
		// TODO Auto-generated method stub
		JavaPairRDD<String, Tuple2<String, String>> blocks = personIndex.flatMapToPair(
	    		
		    	new PairFlatMapFunction<Tuple2<String,Set<String>>,String,Tuple2<String, String>>(){

				private static final long serialVersionUID = 1L;

				@Override
				public Iterable<Tuple2<String,Tuple2<String, String>>> call(Tuple2<String, Set<String>> instancePair) throws Exception {
					// TODO Auto-generated method stub
					ArrayList<Tuple2<String,Tuple2<String, String>>> pairs = 
							new ArrayList<Tuple2<String,Tuple2<String, String>>>();

					String[] predicates = new String[3];
					
					predicates[0] = "http://xmlns.com/foaf/0.1/firstName";//"foaf:firstName";
					predicates[1] = "http://xmlns.com/foaf/0.1/lastName";//"foaf:lastName";
					//predicates[2] = "#foaf:name";
					
					String key = null;
					Tuple2<String,String> t;
					for(int i = 0; i < predicates.length-1;i++){
						key = getPredicateObjectFromNT(instancePair._2,predicates[i]);
						if(key != null){
							t = new Tuple2<String,String>(key,instancePair._1);
							pairs.add(new Tuple2<String,Tuple2<String,String>>(key,t));
							blockedEntities.add(1);
						}else{
							unblockedEntities.add(1);
						}
					}
					if(pairs.size() == 0){
						key = getPredicateValue(instancePair._2,predicates[predicates.length-1]);
						if(key != null){
							pairs.add(new Tuple2<String,String>(key,instancePair._1));
						}else{
							pairs.add(new Tuple2<String,String>("",""));
						//}
					}
					return pairs;	
				}

				

		    });
		
		return null;
	}*/
	
	
	private static String getPredicateValueFromNT(String s, String predicate) {
		// TODO Auto-generated method stub
		int pos = s.indexOf(predicate);
		if(pos == -1)
			return null;
		s = s.substring(pos+predicate.length()+1);
		int pos1 = s.indexOf("^");
		String value = s.substring(0, pos1);
		return value;
		
	}
	
	
	
	private static String getPredicateObjectFromNT(List<String> POs, String predicate) {
		// TODO Auto-generated method stub
		int index = POs.indexOf(predicate);
		if(index == -1){
			//logger.warn(predicate + " not found in list "+POs);
			return null;
		}
		if(index + 1 == POs.size()){
			logger.warn("malformed POs list "+ POs);
			return null;
		}
			
		String o = POs.get(index+1);
		int index1 = o.indexOf("^");
		o = o.substring(0, index1);
		return o;
	}
	
	private static String getPredicateValue(String s,String predicate){
		
		Pattern predicatePtrn = Pattern.compile(predicate+" (.*);\\n"+"|"+predicate+" (.*)");
		
		Matcher matcher = predicatePtrn.matcher(s);
		
		String value = null;
		if(matcher.find()){
			value = matcher.group(1);
			if(value == null){
				value = matcher.group(2);
			}
		}
		return value;
	}
	
	public static JavaPairRDD<String, String> runWithOneProperty(JavaPairRDD<String, String> index){
		
		JavaPairRDD<String, String> blocks = index.mapToPair(
	    		
		    	new PairFunction<Tuple2<String,String>,String,String>(){

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(Tuple2<String, String> instancePair) throws Exception {
					// TODO Auto-generated method stub
					
					String foafname = "#foaf:name";
					
					Pattern namePtrn = Pattern.compile("\\{\""+foafname+"\":\"(.*?)\"\\}");
					
					
					Matcher matcher = namePtrn.matcher(instancePair._2);
					matcher.find();
					String name = matcher.group(1);
					String key = name.substring(0, name.indexOf("^^xsd:string")).replace("'", "");
					return new Tuple2<String,String>(key,instancePair._1);
					/*String[] po_pairs = t._2.split("???");
					
					ArrayList<Tuple2<String, String>> values = new ArrayList<Tuple2<String, String>>();
					
					String v = null;
					Tuple2<String,String> tuple = null;
					
					for(int i = 0; i < po_pairs.length; i++){
						v = po_pairs[i].split("###")[1];
						tuple = new Tuple2<String,String>(v,t._1);
						values.add(tuple);
					}
					return values;*/
				}
		    	
		    });
			blocks = blocks.reduceByKey(new Function2<String,String,String>(){

				private static final long serialVersionUID = 1L;
	
				@Override
				public String call(String instance1, String instance2) throws Exception {
					// TODO Auto-generated method stub
					return instance1+"@@"+instance2;
				}
		    	
		    });
		    return blocks;  
	}
	
}

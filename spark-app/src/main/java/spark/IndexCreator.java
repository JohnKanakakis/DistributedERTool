package spark;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.io.config.KBInfo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.help.DataFormatter;

/**
 * IndexCreator generates the tokenPairs RDD and the resourceIndex RDD
 * @author John Kanakakis
 *
 */
public class IndexCreator {

	
	public static Logger logger = LoggerFactory.getLogger(IndexCreator.class);
	
	/*public static void main(String args[]){
		String s1 = "A Java based tool for the 0-9 design of-class-ification microarrays. ";
		String s2 = "A Java based tool for the design of classification microarrays ";
		
		String[] tokens = s1.replaceAll("[^A-Za-z0-9 ]", " ").split(" ");
		
		for(int i = 0; i < tokens.length; i++){
			tokens[i] = tokens[i].trim().toLowerCase();
			System.out.println(tokens[i]+"#");
		}
		tokens = s2.replaceAll("[^A-Za-z0-9 ]", " ").split(" ");
		for(int i = 0; i < tokens.length; i++){
			tokens[i] = tokens[i].replaceAll("[^A-Za-z0-9 ]", " ").trim().toLowerCase();
			System.out.println(tokens[i]+"#");
		}
	}*/

	/**
	 * @param resources : RDD in the form of (r_id, [info])
	 * @param skbB : source KBInfo
	 * @param tkbB : target KBInfo
	 * @return tokenPairs in the form of (token, r_id)
	 */
	public static JavaPairRDD<String, String> getTokenPairs(JavaPairRDD<String,List<String>> resources,
																  final Broadcast<byte[]> skbB,
																  final Broadcast<byte[]> tkbB)
	{
		final KBInfo skb = (KBInfo)Utils.deserialize(skbB.getValue());
		final KBInfo tkb = (KBInfo)Utils.deserialize(tkbB.getValue());
		final Set<String> sourceProperties = new HashSet<String>(skb.getProperties());
		final Set<String> targetProperties = new HashSet<String>(tkb.getProperties());
		
		PairFlatMapFunction<Iterator<Tuple2<String, List<String>>>, String, String> f = 
		
		// pair = (BKV, (valueOfProperty, r_id) )		
		new PairFlatMapFunction<Iterator<Tuple2<String,List<String>>>,String,String>(){
		
			private static final long serialVersionUID = 1L;
			
			@Override
			public Set<Tuple2<String, String>> call(Iterator<Tuple2<String, 
													List<String>>> resources)
			throws Exception {
			
				HashSet<Tuple2<String,String>> tokenPairs = 
						new HashSet<Tuple2<String,String>>();
				
				Set<String> kbProperties = null;
				Tuple2<String, List<String>> resource = null;
				String r_id = null;
				List<String> r_info = null;
				String predicate = null;
				String object = null;
				String BKV = "";
				Tuple2<String,String> t;
				String datasetId = "";
				String[] tokens;
				
				while(resources.hasNext()){
					resource = resources.next();
					r_id = resource._1;
					r_info = resource._2;
					datasetId = DatasetManager.getDatasetIdOfResource(r_id);
					if(datasetId.equals(tkb.getId())){
						kbProperties = targetProperties;
					}else if(datasetId.equals(skb.getId())){
						kbProperties = sourceProperties;
					}
					//clear BKV
					BKV = "";
					for(int i = 0; i < r_info.size()-1; i = i + 2){
						predicate = r_info.get(i);
						object = DataFormatter.eliminateDataTypeFromLiteral(r_info.get(i+1));
						object = object.replace("\"", "");
						if(kbProperties.contains(predicate)){
							object = DataFormatter.eliminateDataTypeFromLiteral(object);
							BKV+=(object.replace("\"", "")+" ");
						}
					}
					tokens = BKV.toLowerCase().replaceAll("[^A-Za-z0-9 ]", " ").split(" ");
					
					for(int i = 0; i < tokens.length; i++){
						tokens[i] = tokens[i].trim();
						t = new Tuple2<String,String>(tokens[i],r_id);
						tokenPairs.add(t);
					}
					
				}
				return tokenPairs;	
			}
		};
		JavaPairRDD<String, String> result = resources.mapPartitionsToPair(f);
		return result;
	}
	
	
	

	/**
	 * @param tokenPairs RDD in the form of (block_key, r_id)
	 * @return the resourceIndex RDD in the form of (r_id, (block_key, r_id) )
	 */
	public static JavaPairRDD<String, Tuple2<String, String>> createIndex(JavaPairRDD<String, String> tokenPairs) {
		JavaPairRDD<String, Tuple2<String, String>> index 
		= tokenPairs.mapToPair(new PairFunction<Tuple2<String,String>,String,Tuple2<String,String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> tokenPair) 
			throws Exception {
				String resourceId = tokenPair._2;
				return new Tuple2<String, Tuple2<String, String>>(resourceId,tokenPair);
			}
		});
		
		return index;
	}
}

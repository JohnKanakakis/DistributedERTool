package spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.aksw.limes.core.measures.measure.string.Jaro;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.Partitioner;
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

import com.google.common.base.Optional;

import it.unimi.dsi.fastutil.*;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashBigSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import scala.Tuple2;
import spark.blocking.BlockCreator;
import spark.blocking.BlockReader;
import spark.io.DataReader;
import spark.io.InstanceIndexReader;

public class BlocksCreator {

	public static final int W = 20;
	public static Logger logger = LoggerFactory.getLogger(BlockCreator.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("Controller");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	Logger logger = sparkConf.log();
    	
		JavaRDD<String> dataBlocks = ctx.textFile(args[0]);
		JavaPairRDD<String, Tuple2<String, String>> blocks = BlockReader.run(dataBlocks);
		
		
		
		Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("textinputformat.record.delimiter", ")\n");
      
        JavaRDD<String> indexData = DataReader.run(ctx.newAPIHadoopFile(args[1], 
														 TextInputFormat.class, 
														 LongWritable.class, 
														 Text.class,
														 conf));
        JavaPairRDD<String, List<String>> personIndex = InstanceIndexReader.run(indexData);
        
        JavaPairRDD<String, Set<List<String>>> blocksWithInfo = enrichBlocks(blocks,personIndex);
		blocksWithInfo.saveAsTextFile(args[2]);
		ctx.close();
	}

	private static JavaPairRDD<String, Set<List<String>>> enrichBlocks(JavaPairRDD<String, Tuple2<String, String>> blocks,
			                                                 JavaPairRDD<String,List<String>> personIndex){
			
			Function2<Set<List<String>>,List<String>,Set<List<String>>> seqFunc = 
					new Function2<Set<List<String>>,List<String>,Set<List<String>>>(){
						private static final long serialVersionUID = 1L;
						@Override
						public Set<List<String>> call(Set<List<String>> personInfoListPerKey, 
								List<String>personInfo) 
						throws Exception {
							// TODO Auto-generated method stub
							personInfoListPerKey.add(personInfo);
							return personInfoListPerKey;
						}
			};
			
			Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>> combFunc = 
					new Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>>(){
						private static final long serialVersionUID = 1L;
						@Override
						public Set<List<String>> call(Set<List<String>> personInfoListPerKey_1, 
												Set<List<String>> personInfoListPerKey_2) 
						throws Exception {
							// TODO Auto-generated method stub
							personInfoListPerKey_1.addAll(personInfoListPerKey_2);
							return personInfoListPerKey_1;
						}
			};
			 
			/* (r_id, [info]) join (r_id, (block_key,r_id) ) =>
			 * (r_id, ((block_key,r_id), [info]) ) =>
			 * (block_key, [r_id|info])
			 */
			return 
					blocks.join(personIndex)
						  .mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple2<String, String>,List<String>>>,String,List<String>>(){
								private static final long serialVersionUID = 1L;
						
								@Override
								public Tuple2<String,List<String>> call(
										Tuple2<String,Tuple2<Tuple2<String, String>, List<String>>> pair) throws Exception {
									// TODO Auto-generated method stub
									Tuple2<String, String> wordResourceId_pair = pair._2._1;
									
									List<String> info = pair._2._2;
									
									String word = wordResourceId_pair._1;
									String r_id = wordResourceId_pair._2;
									if(info.size()%2 != 0){
										//logger.warn("we have examined that list "+info);
										//return null;
									}else{
										info.add(0, r_id);
									}
									//logger.info("arraylist before = "+info);
									
									//logger.info("arraylist after = "+info);
									//Tuple2<String,String> personId_info_pair = new Tuple2<String,String>(person_id,info);
									
									return new Tuple2<String, List<String>>(word,info);
								}
						  })
						  .aggregateByKey(new ObjectOpenHashBigSet<List<String>>(),seqFunc,combFunc);
	
		
	}

	

	public static JavaPairRDD<String, Set<List<String>>> createBlocks( JavaPairRDD<String, Tuple2<String, String>> resourceIndex,
																		    JavaPairRDD<String, List<String>> resources) {
		// TODO Auto-generated method stub
		
		/*PairFlatMapFunction<Tuple2<String, ObjectOpenHashBigSet<Tuple2<String, String>>>, String, Tuple2<String, String>> tokenBlocking = 
		new PairFlatMapFunction<Tuple2<String, ObjectOpenHashBigSet<Tuple2<String, String>>>, String,Tuple2<String,String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String,Tuple2<String,String>>> call(Tuple2<String, ObjectOpenHashBigSet<Tuple2<String, String>>> indexBlock) throws Exception {
				
				String BKV = indexBlock._1;
				String r_id;
				Tuple2<String, String> t;
				HashSet<Tuple2<String,Tuple2<String,String>>> result = 
						new HashSet<Tuple2<String,Tuple2<String,String>>>();
				for(Tuple2<String, String> r : indexBlock._2){
					r_id = r._2;
					t = new Tuple2<String,String>(BKV,r_id);
					result.add(new Tuple2<String,Tuple2<String,String>>(r_id,t));
				}
				return result;
			
			}
		};*/
		Function2<Set<List<String>>,List<String>,Set<List<String>>> seqFunc = 
				new Function2<Set<List<String>>,List<String>,Set<List<String>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<List<String>> call(Set<List<String>> personInfoListPerKey, 
							List<String>personInfo) 
					throws Exception {
						// TODO Auto-generated method stub
						personInfoListPerKey.add(personInfo);
						return personInfoListPerKey;
					}
		};
		
		Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>> combFunc = 
				new Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<List<String>> call(Set<List<String>> personInfoListPerKey_1, 
											Set<List<String>> personInfoListPerKey_2) 
					throws Exception {
						// TODO Auto-generated method stub
						personInfoListPerKey_1.addAll(personInfoListPerKey_2);
						return personInfoListPerKey_1;
					}
		};
		//ObjectOpenHashBigSet<Tuple2<String,String>> zeroValue = new ObjectOpenHashBigSet<Tuple2<String,String>>();
		//JavaPairRDD<String, Tuple2<String, String>> tokenPairs = resourceIndex.flatMapToPair(tokenBlocking);
	
		
		 
		/* (r_id, (block_key,r_id) ) join (r_id, [info])  =>
		 * (r_id, ((block_key,r_id), [info]) ) =>
		 * (block_key, [r_id|info])
		 */
		return 
				resourceIndex.join(resources)
					  .mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple2<String, String>,List<String>>>,String,List<String>>(){
							private static final long serialVersionUID = 1L;
					
							@Override
							public Tuple2<String,List<String>> call(
									Tuple2<String,Tuple2<Tuple2<String, String>, List<String>>> pair) throws Exception {
								// TODO Auto-generated method stub
								Tuple2<String, String> wordResourceId_pair = pair._2._1;
								
								List<String> info = pair._2._2;
								
								String word = wordResourceId_pair._1;
								String r_id = wordResourceId_pair._2;
								if(info.size()%2 != 0){
									//logger.warn("we have examined that list "+info);
									//return null;
								}else{
									info.add(0, r_id);
								}
								//logger.info("arraylist before = "+info);
								
								//logger.info("arraylist after = "+info);
								//Tuple2<String,String> personId_info_pair = new Tuple2<String,String>(person_id,info);
								
								return new Tuple2<String, List<String>>(word,info);
							}
					  })
					  .aggregateByKey(new ObjectOpenHashBigSet<List<String>>(),seqFunc,combFunc)
					  .filter(new Function<Tuple2<String,Set<List<String>>>,Boolean>(){
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(Tuple2<String, Set<List<String>>> block) throws Exception {
							// TODO Auto-generated method stub
							return (block._2.size() > 1);
						}
						  
					  });
	}

	public static JavaPairRDD<String, Tuple2<String, String>> createWindows(JavaPairRDD<String, ObjectOpenHashBigSet<Tuple2<String, String>>> resourceIndex,
							         Broadcast<HashMap<String, Set<Integer>>> w_bkv_B) {
		// TODO Auto-generated method stub

		final HashMap<String, Set<Integer>> w_bkv = w_bkv_B.getValue();
		
		PairFlatMapFunction<Tuple2<String, ObjectOpenHashBigSet<Tuple2<String, String>>>, String, Tuple2<String, String>> sortedNeighborBlocking = 
				new PairFlatMapFunction<Tuple2<String, ObjectOpenHashBigSet<Tuple2<String, String>>>, String,Tuple2<String,String>>(){
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String,Tuple2<String,String>>> call(Tuple2<String, ObjectOpenHashBigSet<Tuple2<String, String>>> indexBlock) throws Exception {
						// TODO Auto-generated method stub
						String BKV = indexBlock._1;
						HashSet<Tuple2<String,Tuple2<String,String>>> result = 
								new HashSet<Tuple2<String,Tuple2<String,String>>>();
						//Comparator<Tuple2<String, String>> c = new Tuple2Comparator();
						Set<Integer> windows = w_bkv.get(BKV);
						//logger.info(w_bkv.toString());
						// ( r_id , (w, r_id) )
						Tuple2<String,String> t;
						String r_id;
						for(Tuple2<String, String> r : indexBlock._2){
							r_id = r._2;
							for(int w : windows){
								t = new Tuple2<String,String>("w"+w,r_id);
								result.add(new Tuple2<String,Tuple2<String,String>>(r_id,t));
							}
						}
						
						return result;
						
					}
		};
		return resourceIndex.flatMapToPair(sortedNeighborBlocking);
	}

	public static JavaPairRDD<String, Set<List<String>>> purge(JavaPairRDD<String, Set<List<String>>> blocks,
															   final int threshold) {
		// TODO Auto-generated method stub

		return blocks.filter(new Function<Tuple2<String,Set<List<String>>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<List<String>>> block) throws Exception {
				// TODO Auto-generated method stub
				return (block._2.size() <= threshold);
			}
			
		});
		
		
		
	}
	
}


/*
private static void getFrequencyOfPersonIdsPerpartition(){
	JavaPairRDD<String, Integer> uniquePersonIdsPerPartition = blocks.mapPartitionsToPair(
	new PairFlatMapFunction<Iterator<Tuple2<String,String>>,String,Integer>(){

		private static final long serialVersionUID = 1L;
		@Override
		public Iterable<Tuple2<String,Integer>> call(Iterator<Tuple2<String, String>> localBlockSet) 
				throws Exception 
		{
			// TODO Auto-generated method stub
			HashMap<String,Integer> uniqueLocalPersonIds = new HashMap<String,Integer>();
			
			Tuple2<String, String> t = null;
			String p_id = null;
			int count = -1;
			while(localBlockSet.hasNext()){
				t = localBlockSet.next();
				p_id = t._2;
				
				if(!uniqueLocalPersonIds.containsKey(p_id)){
					count = 1;
				}else{
					count = uniqueLocalPersonIds.get(p_id);
					count++;
				}
				uniqueLocalPersonIds.put(p_id, count);
			}
			
			ArrayList<Tuple2<String,Integer>> result = new ArrayList<Tuple2<String,Integer>>();
			Iterator<Entry<String,Integer>> set = uniqueLocalPersonIds.entrySet().iterator();
			Entry<String,Integer> e = null;
			
			while(set.hasNext()){
				e = set.next();
				result.add(new Tuple2<String,Integer>(e.getKey(),e.getValue()));
			}
			return result;
		}
	});*/
/*class CustomPairTuple implements Serializable{

	private static final long serialVersionUID = 1L;
	public String _1;
	public String _2;
}
class Tuple2Comparator implements Comparator<Tuple2<String,String>>{
	
	public int compare(Tuple2<String, String> t1, Tuple2<String, String> t2) {
		// TODO Auto-generated method stub
		String p1 = t1._1;
		String p2 = t2._1;
		return p1.compareTo(p2);
	}
}

class CustomPairComparator implements Comparator<CustomPairTuple>{

	public int compare(Tuple2<String, String> t1, Tuple2<String, String> t2) {
		// TODO Auto-generated method stub
		String p1 = t1._2;
		String p2 = t2._2;
		return p1.compareTo(p2);
	}

	@Override
	public int compare(Object arg0, Object arg1) {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		Tuple2<String, String> t1 = (Tuple2<String, String>) arg0;
		
		
		return 0;
	}

	@Override
	public int compare(CustomPairTuple t1, CustomPairTuple t2) {
		// TODO Auto-generated method stub
		String p1 = t1._2;
		String p2 = t2._2;
		return p1.compareTo(p2);
	}

}*/

/*blocks.setName("blocks");
blocks.cache();

personIndex.setName("personIndex");
personIndex.cache();
*/

//logger.info("total blocks = "+blocks.count());

/*JavaPairRDD<String, Integer> uniquePersonIds = 
		blocks.keys()
			  .distinct()
			  .mapToPair(new PairFunction<String,String,Integer>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, Integer> call(String p_id) throws Exception {
					// TODO Auto-generated method stub
					return new Tuple2<String,Integer>(p_id,1);
				}
});*/
//block = (p_id,(w,p_id))
//personIndex = (p_id,[info])

/*JavaPairRDD<String, Integer> uniquePersonIds = 
	blocks.keys().mapPartitions(
		new FlatMapFunction<Iterator<String>,String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(Iterator<String> localBlockSet) 
			throws Exception 
			{
				HashMap<String,Integer> uniqueLocalPersonIds = new HashMap<String,Integer>();
				String p_id = null;
				int count = -1;
				while(localBlockSet.hasNext()){
					p_id = localBlockSet.next();
					if(!uniqueLocalPersonIds.containsKey(p_id)){
						count = 1;
					}else{
						count = uniqueLocalPersonIds.get(p_id);
						count++;
					}
					uniqueLocalPersonIds.put(p_id, count);
				}
				return uniqueLocalPersonIds.keySet();
			}
}).mapToPair(new PairFunction<String,String,Integer>(){
	private static final long serialVersionUID = 1L;
	@Override
	public Tuple2<String, Integer> call(String p_id) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2<String,Integer>(p_id,1);
	}
}).cache();
//logger.info("unique ids = "+uniquePersonIds.count());

//lookupJoinIndex = (p_id, [info])
JavaPairRDD<String,List<String>> lookupJoinIndex = uniquePersonIds.join(personIndex)
																	.mapValues(
				new Function<Tuple2<Integer,List<String>>,List<String>>(){

						private static final long serialVersionUID = 1L;
						@Override
						public List<String> call(Tuple2<Integer, List<String>> pair) throws Exception {
							// TODO Auto-generated method stub
							return pair._2;
						}
			
});
lookupJoinIndex.cache();
//personIndex.unpersist();

//lookupJoinIndex.setName("lookupJoinIndex");
//lookupJoinIndex.cache();

Function2<Set<List<String>>,List<String>,Set<List<String>>> seqFunc = 
		new Function2<Set<List<String>>,List<String>,Set<List<String>>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Set<List<String>> call(Set<List<String>> personInfoListPerKey, 
					List<String>personInfo) 
			throws Exception {
				// TODO Auto-generated method stub
				personInfoListPerKey.add(personInfo);
				return personInfoListPerKey;
			}
};

Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>> combFunc = 
		new Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Set<List<String>> call(Set<List<String>> personInfoListPerKey_1, 
									Set<List<String>> personInfoListPerKey_2) 
			throws Exception {
				// TODO Auto-generated method stub
				personInfoListPerKey_1.addAll(personInfoListPerKey_2);
				return personInfoListPerKey_1;
			}
};

//lookupJoinIndex = (p_id, [info])
//block = (p_id,(w,p_id))
//blocksWithInfo = (p_id, ( (w,p_id),[info] ) ) => (w,[p_id|info]) => (w,[[p_id|info]])
JavaPairRDD<String, Set<List<String>>> blocksWithInfo = 
			blocks.join(lookupJoinIndex)
				  .values()
			      .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>,List<String>>,
			    		                      String,
			    		                      List<String>>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String,List<String>> call(
							Tuple2<Tuple2<String, String>, List<String>> pair) throws Exception {
						// TODO Auto-generated method stub
						Tuple2<String, String> word_PersonId_Pair = pair._1;
						
						List<String> info = pair._2;
						
						String word = word_PersonId_Pair._1;
						String person_id = word_PersonId_Pair._2;
						
						//logger.info("arraylist before = "+info);
						info.add(0, person_id);
						//logger.info("arraylist after = "+info);
						//Tuple2<String,String> personId_info_pair = new Tuple2<String,String>(person_id,info);

						return new Tuple2<String, List<String>>(word,info);
					}
			      }).aggregateByKey(new HashSet<List<String>>(),seqFunc,combFunc);

return blocksWithInfo;*/
	
/*
	// TODO Auto-generated method stub
	String BKV = indexBlock._1;
	//Comparator<Tuple2<String, String>> c = new Tuple2Comparator();
	
	
	ArrayList<Tuple2<String, String>> l = new ArrayList<Tuple2<String, String>>(indexBlock._2);
	//Collections.sort(l, c);
	long L = l.size();

	//logger.info(l.toString());
	Tuple2<String,String> indexBlockPair = null;
	Tuple2<String,Tuple2<String,String>> blockTuple = null;
	String r_id = null;
	List<Tuple2<String,Tuple2<String,String>>> result = new ArrayList<Tuple2<String,Tuple2<String,String>>>();
	
	for(int i = 0; i < Math.max(L-W+1,1); i++){
		for(int j = i; j < Math.min(i+W,L); j++){
			indexBlockPair = l.get(j);
			r_id = indexBlockPair._2;
			blockTuple = new Tuple2<String,Tuple2<String,String>>(r_id,
																  new Tuple2<String,String>(BKV+"_"+i,r_id));
			result.add(blockTuple);
		}
	}
	
	Jaro j = new Jaro();
	int windowNr = 0; 
	double sim;
	double thres = 0.5;
	r_id = l.get(0)._2;
	blockTuple = new Tuple2<String,Tuple2<String,String>>(r_id,
			  new Tuple2<String,String>(BKV+"_"+windowNr,r_id));
	result.add(blockTuple);
	for(int i = 1; i < L; i++){
		//sim = jaro(e[i],e[i-1]); 
		sim = j.getSimilarity(l.get(i)._1, l.get(i-1)._1);
		if(sim < thres){
			windowNr++;
		}
		//e[i] goes to window W	
		r_id = l.get(i)._2;
		blockTuple = new Tuple2<String,Tuple2<String,String>>(r_id,
				  new Tuple2<String,String>(BKV+"_"+windowNr,r_id));
		result.add(blockTuple);
	}
	logger.info((windowNr+1)+" total windows with average size "+L/(windowNr+1));
	return result;
}
	*/
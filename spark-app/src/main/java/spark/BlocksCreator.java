package spark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashBigSet;
import scala.Tuple2;


public class BlocksCreator {

	public static final int W = 20;
	public static Logger logger = LoggerFactory.getLogger(BlocksCreator.class);
	
	
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
package spark;

import java.io.InputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import scala.Tuple2;
import spark.statistics.BlockStatistics;

class Statistics implements Serializable{

	private static final long serialVersionUID = 1L;
	int optimalBlockSize;
	BigInteger blocksBeforePurging;
	BigInteger blocksAfterPurging;
	double durationOfLinking;
	BigInteger defaultNumberOfComparisons; 
	BigInteger numberOfComparisonsWithoutPurging; 
	BigInteger numberOfComparisonsWithPurging; 
}

public class Controller {

	private static String STATS_FILE;
	public static Logger logger = LoggerFactory.getLogger(Controller.class);
	static SparkConf sparkConf;
	static JavaSparkContext ctx;
	static Statistics statistics;
	static boolean statisticsEnabled;
	static boolean purging_enabled;
	static boolean linkingEnabled;
	public static void main(String[] args) {

		statisticsEnabled = Boolean.parseBoolean(args[3]);
		purging_enabled = Boolean.parseBoolean(args[4]);
		linkingEnabled = Boolean.parseBoolean(args[5]);
		if(statisticsEnabled)
			statistics = new Statistics();


		STATS_FILE = args[2];
		Utils.deleteHDFSFile(STATS_FILE);

		InputStream configFile = Utils.getHDFSFile(args[0]);
		InputStream dtdFile = Utils.getHDFSFile(args[1]);

		XMLConfigurationReader reader = new XMLConfigurationReader();
		org.aksw.limes.core.io.config.Configuration config = reader.validateAndRead(configFile,dtdFile);



		if(config == null){
			System.exit(0);
		}
		if(linkingEnabled){
			Utils.deleteHDFSFile(config.getAcceptanceFile());
			Utils.deleteHDFSFile(config.getAcceptanceFile()+".ser");
		}


		Rewriter rw = RewriterFactory.getRewriter("Default");
		LinkSpecification ls = new LinkSpecification(config.getMetricExpression(), config.getVerificationThreshold());
		LinkSpecification rwLs = rw.rewrite(ls);
		IPlanner planner = ExecutionPlannerFactory.getPlanner(config.getExecutionPlan(), null, null);
		assert planner != null;
		NestedPlan plan = planner.plan(rwLs);

		byte[] planBinary = Utils.serialize(plan);
		byte[] configBinary = Utils.serialize(config);
		byte[] skbBinary = Utils.serialize(config.getSourceInfo());
		byte[] tkbBinary = Utils.serialize(config.getTargetInfo());

		//String[] configParams = new String[2];
		// configParams[0] = config.getSourceInfo().getVar();
		//configParams[1] = config.getTargetInfo().getVar();

		// double thres = config.getAcceptanceThreshold();




		sparkConf = new SparkConf().setAppName("Controller");

		ctx = new JavaSparkContext(sparkConf);


		Broadcast<byte[]> planBinary_B = ctx.broadcast(planBinary);
		Broadcast<byte[]> configBinary_B = ctx.broadcast(configBinary);

		//Broadcast<String[]> c = ctx.broadcast(configParams);
		//Broadcast<Double> t = ctx.broadcast(thres);
		Broadcast<byte[]> skb = ctx.broadcast(skbBinary);
		Broadcast<byte[]> tkb = ctx.broadcast(tkbBinary);


		JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> records1 = 
				ctx.objectFile(config.getSourceInfo().getEndpoint());

		JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> records2 = 
				ctx.objectFile(config.getTargetInfo().getEndpoint());


		JavaPairRDD<String, List<String>> resources1 = 
				ResourceFilter.runWithPairs(records1,skb)
				.setName("resource1");
		//.persist(StorageLevel.MEMORY_ONLY_SER());
		JavaPairRDD<String, List<String>> resources2 = 
				ResourceFilter.runWithPairs(records2,tkb)
				.setName("resource2");
		//.persist(StorageLevel.MEMORY_ONLY_SER());


		JavaPairRDD<String, List<String>> resources = 
				resources1.union(resources2)
				.setName("resources")
				.persist(StorageLevel.MEMORY_ONLY_SER());





		JavaPairRDD<String, String> tokenPairs = 
				IndexCreator.getTokenPairs(resources,skb,tkb);
		//.persist(StorageLevel.MEMORY_ONLY_SER());


		JavaPairRDD<String, Integer> blockSizesRDD = 
				getBlocks(tokenPairs)
				.setName("blockSizesRDD")
				.persist(StorageLevel.MEMORY_ONLY_SER());

		if(statisticsEnabled){
			statistics.blocksBeforePurging = 
					BigInteger.valueOf(blockSizesRDD.count());
			statistics.numberOfComparisonsWithoutPurging = 
					getNumberOfComparisons(tokenPairs,skb,tkb);
		}


		final int optimalSize = BlockStatistics.getOptimalBlockSize(blockSizesRDD);

		if(statisticsEnabled){
			statistics.optimalBlockSize = optimalSize;
		}



		blockSizesRDD = blockSizesRDD.filter(new Function<Tuple2<String,Integer>,Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String,Integer> block) 
			throws Exception {
				// TODO Auto-generated method stub
				return (block._2 > 1 && block._2 <= optimalSize);
			}
		});


		HashSet<String> localPurgedBlockKeysSet = new HashSet<String>(blockSizesRDD.keys().collect());

		final Broadcast<HashSet<String>> broadcastedPurgedBlockKeys = 
				ctx.broadcast(localPurgedBlockKeysSet);


		if(purging_enabled){
			tokenPairs 
			= tokenPairs.filter(new Function<Tuple2<String,String>,Boolean>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, String> indexPair) throws Exception {
					String blockKey = indexPair._1;
					return broadcastedPurgedBlockKeys.getValue().contains(blockKey);
				}
			});
		}


		if(statisticsEnabled){
			statistics.blocksAfterPurging = BigInteger.valueOf(blockSizesRDD.count());
			statistics.numberOfComparisonsWithPurging = getNumberOfComparisons(tokenPairs,skb,tkb);
		}

		JavaPairRDD<String, Tuple2<String, String>> resourceIndex = IndexCreator.createIndex(tokenPairs);



		JavaPairRDD<String, Set<List<String>>> blocks = BlocksCreator.createBlocks(resourceIndex,resources);

		JavaPairRDD<String, String> links = null;
		if(linkingEnabled){
			links = Linker.run(blocks, planBinary_B, configBinary_B);
			links.persist(StorageLevel.MEMORY_ONLY_SER());

			links.saveAsTextFile(config.getAcceptanceFile());
			links.saveAsObjectFile(config.getAcceptanceFile()+".ser");
		}


		if(statisticsEnabled){
			ArrayList<String> result = new ArrayList<String>();
			if(linkingEnabled)
				result.add("links = "+links.count());

			result.add("optimal block size = "+statistics.optimalBlockSize);
			result.add("blocks before = "+statistics.blocksBeforePurging);
			result.add("blocks after = "+statistics.blocksAfterPurging);

			long numOfResources1 = resources1.count();
			long numOfResources2 = resources2.count();

			statistics.defaultNumberOfComparisons = BigInteger.valueOf(numOfResources1).multiply(BigInteger.valueOf(numOfResources2));

			result.add("resources1 = "+numOfResources1);
			result.add("resources2 = "+numOfResources2);
			result.add("default number of comparisons (without purging) = "+ statistics.defaultNumberOfComparisons.toString());
			result.add("total number of comparisons (without purging) = "+ statistics.numberOfComparisonsWithoutPurging.toString());
			result.add("total number of comparisons (with purging) = "+ statistics.numberOfComparisonsWithPurging.toString());

			ctx.parallelize(result).coalesce(1).saveAsTextFile(STATS_FILE);
		}

		ctx.close();
	}

	private static JavaPairRDD<String, Integer> getBlocks(JavaPairRDD<String,String> tokenPairs){

		Function<String, Integer> createCombiner 
		= new Function<String, Integer>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(String token) throws Exception {
				// TODO Auto-generated method stub
				return 1;
			}
		};

		Function2<Integer, String, Integer> mergeValue 
		= new Function2<Integer, String, Integer>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, String v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+1;
			}
		};

		Function2<Integer, Integer, Integer> mergeCombiners 
		= new Function2<Integer, Integer, Integer>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		};
		//(block_key,N)
		JavaPairRDD<String, Integer> blockSizesRDD = 
				tokenPairs.combineByKey(createCombiner,mergeValue,mergeCombiners);

		return blockSizesRDD;

	}
	private static BigInteger getNumberOfComparisons(JavaPairRDD<String, String> tokenPairs,
			final Broadcast<byte[]> skb,
			final Broadcast<byte[]> tkb) {

		Function2<Set<String>, String, Set<String>> secFunc
		= new Function2<Set<String>, String, Set<String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Set<String> call(Set<String> set, String resourceId) throws Exception {
				set.add(resourceId);
				return set;
			}
		};

		Function2<Set<String>, Set<String>, Set<String>> combFunc
		= new Function2<Set<String>, Set<String>, Set<String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Set<String> call(Set<String> set1, Set<String> set2) throws Exception {
				set1.addAll(set2);
				return set1;
			}
		};

		JavaPairRDD<String, Set<String>> blocks = 
				tokenPairs.aggregateByKey(new HashSet<String>(), secFunc,combFunc);

		AccumulatorParam<BigInteger> accParam = new AccumulatorParam<BigInteger>(){

			private static final long serialVersionUID = 1L;

			@Override
			public BigInteger addInPlace(BigInteger arg0, BigInteger arg1) {
				return arg0.add(arg1);
			}

			@Override
			public BigInteger zero(BigInteger arg0) {
				return BigInteger.ZERO;
			}

			@Override
			public BigInteger addAccumulator(BigInteger arg0, BigInteger arg1) {
				return arg0.add(arg1);
			}
		};
		final Accumulator<BigInteger> numberOfComparisons = ctx.accumulator(BigInteger.ZERO,accParam);

		final KBInfo sourceKb = (KBInfo) Utils.deserialize(skb.value());
		final KBInfo targetKb = (KBInfo) Utils.deserialize(tkb.value());

		blocks.foreach(new VoidFunction<Tuple2<String,Set<String>>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Set<String>> blockPair) throws Exception {
				// TODO Auto-generated method stub
				long cnt1 = 0;
				long cnt2 = 0;
				String datasetId;
				for(String resourceId : blockPair._2){
					datasetId = DatasetManager.getDatasetIdOfResource(resourceId);
					if(datasetId.equals(sourceKb.getId())){
						cnt1++;
					}else if(datasetId.equals(targetKb.getId())){
						cnt2++;
					}
				}
				numberOfComparisons.add(BigInteger.valueOf(cnt1).multiply(BigInteger.valueOf(cnt2)));
			}
		});
		return numberOfComparisons.value();
	}
}


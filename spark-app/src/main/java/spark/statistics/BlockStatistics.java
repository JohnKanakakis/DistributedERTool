package spark.statistics;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


import scala.Tuple2;

public class BlockStatistics {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("Controller");
    	
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
    	
    	JavaRDD<String> lines = ctx.textFile(args[0]);
    	
    	
    	JavaRDD<String> blockLines = 
    	
    	lines.filter(new Function<String,Boolean>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String line) throws Exception {
				// TODO Auto-generated method stub
				int p = line.indexOf("block size = ");
				if(p == -1)
					return false;
				
				line = line.substring(p);
				
				Pattern pat = Pattern.compile("block size = (\\d*)$");
				Matcher matcher = pat.matcher(line);
				if(matcher.find()){
					return true;
				}
				return false;
			}
    	});
    	
    	
    	blockLines.mapToPair(new PairFunction<String,Integer,Integer>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer,Integer> call(String line) throws Exception {
				// TODO Auto-generated method stub
				int p = line.indexOf("block size = ");
				line = line.substring(p);
				
				Pattern pat = Pattern.compile("block size = (\\d*)$");
				Matcher matcher = pat.matcher(line);
				if(matcher.find()){
					int blockSize = Integer.parseInt(matcher.group(1));
					return new Tuple2<Integer,Integer>(blockSize,1);
				}
				return null;
			}
    	}).foldByKey(0, new Function2<Integer,Integer,Integer>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
    	}).sortByKey().coalesce(1).saveAsTextFile(args[1]);
    	
    	ctx.close();
	}

	public static JavaPairRDD<String, Integer> computeStatistics(JavaPairRDD<String, Set<List<String>>> blocks) {
		// TODO Auto-generated method stub
		
		 
		return 
		blocks.mapValues(new Function<Set<List<String>>,Integer>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Set<List<String>> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.size();
			}
		});
		
		
		
	}

	public static int getOptimalBlockSize(List<Tuple2<Integer, Integer>> bs) {
		// TODO Auto-generated method stub
		BigInteger numberOfComparisons = BigInteger.ZERO;
		BigInteger totalSizeOfBlocks = BigInteger.ZERO;
		BigInteger blockSize;
	
		double CC = 0d;

		CC = 0d;
		
		int lastBlockSize = 2;
		int f;
		
		ArrayList<Tuple2<Integer, Integer>> blockSizes = new ArrayList<Tuple2<Integer, Integer>>(bs);
		Collections.sort(blockSizes,new Comparator<Tuple2<Integer, Integer>>(){

			@Override
			public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
				// TODO Auto-generated method stub
				return Integer.compare(t1._1, t2._1);
			}
			
		});
		
		ArrayList<Tuple2<Integer,Double>> statistics = new ArrayList<Tuple2<Integer,Double>>();
		
		for(int i = 0; i < blockSizes.size(); i++){
			blockSize = new BigInteger(blockSizes.get(i)._1.toString());
			f = Integer.parseInt(blockSizes.get(i)._2.toString());
			if(lastBlockSize < blockSize.intValue()){
				Tuple2<Integer,Double> st = new Tuple2<Integer,Double>(lastBlockSize,CC);
				statistics.add(st);
				lastBlockSize = blockSize.intValue();
			}
			for(int j = 0; j < f; j++){
				totalSizeOfBlocks = totalSizeOfBlocks.add(blockSize);
				numberOfComparisons = numberOfComparisons.add(blockSize.multiply(blockSize.subtract(BigInteger.ONE)).shiftLeft(1));
				CC = totalSizeOfBlocks.doubleValue()/numberOfComparisons.doubleValue();
			}
		}
		Tuple2<Integer,Double> st = new Tuple2<Integer,Double>(lastBlockSize,CC);
		statistics.add(st);
		int optimalBlockSize = lastBlockSize;
		
		double eps = 100d;
		
		for(int i = statistics.size() -1; i >= 2; i--){
			if(Math.abs(statistics.get(i)._2 - statistics.get(i-1)._2) < eps){
				eps = Math.abs(statistics.get(i)._2 - statistics.get(i-1)._2);
			}
		}
		for(int i = statistics.size() -1; i >= 2; i--){
			if(Math.abs(statistics.get(i)._2 - statistics.get(i-1)._2) <= eps){
				optimalBlockSize = statistics.get(i)._1;
				break;
			}
		}
		
		return optimalBlockSize;
	}

	public static JavaPairRDD<String, Integer> filterOfSizeN(
			JavaPairRDD<String, Tuple2<String, String>> resourceIndex, final int N) {
		// TODO Auto-generated method stub
		
		//(block_key,1)
		return
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
        .filter(new Function<Tuple2<String,Integer>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v) throws Exception {
				// TODO Auto-generated method stub
				return v._2 == N;
			}
        });
		
	}

}

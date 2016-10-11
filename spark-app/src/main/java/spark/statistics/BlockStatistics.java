package spark.statistics;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class BlockStatistics {

	/**
	 * @param blockSizesRDD
	 *            (token, blockSize)
	 * @return optimal block size
	 */
	public static int getOptimalBlockSize(
			JavaPairRDD<String, Integer> blockSizesRDD) {

		BigInteger numberOfComparisons = BigInteger.ZERO;
		BigInteger totalSizeOfBlocks = BigInteger.ZERO;
		BigInteger blockSize;

		Function2<Integer, Integer, Integer> addFunction 
		= new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2)
					throws Exception {
				return v1 + v2;
			}
		};

		// (N,freq)
		JavaPairRDD<Integer, Integer> blocksFreq 
		= blockSizesRDD.mapToPair(
				new PairFunction<Tuple2<String, Integer>, Integer, Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Integer, Integer> call(Tuple2<String, Integer> t)
				throws Exception {
					return new Tuple2<Integer, Integer>(t._2, 1);
				}
		})
		.aggregateByKey(0, addFunction,addFunction);

		ArrayList<Tuple2<Integer, Integer>> blockSizesAndFreq 
		= new ArrayList<Tuple2<Integer, Integer>>(blocksFreq.collect());

		/*
		 * blockSizes: sorted array of (blockSize,Frequency) tuples in ascending
		 * order
		 */
		Collections.sort(blockSizesAndFreq,
				new Comparator<Tuple2<Integer, Integer>>() {
					@Override
					public int compare(
							Tuple2<Integer, Integer> t1,
							Tuple2<Integer, Integer> t2) {
						// TODO Auto-generated method stub
						return Integer.compare(t1._1,
								t2._1);
					}
				});

		double CC = 0d;
		CC = 0d;
		int freq;

		/*
		 * statistics: array of pairs (blockSize, CC) for every blockSize
		 */
		ArrayList<Tuple2<Integer, Double>> statistics = 
				new ArrayList<Tuple2<Integer, Double>>();

		for (int i = 0; i < blockSizesAndFreq.size(); i++) {
			blockSize = new BigInteger(
					blockSizesAndFreq.get(i)._1.toString());
			
			freq = blockSizesAndFreq.get(i)._2;
			
			totalSizeOfBlocks = totalSizeOfBlocks
					.add(BigInteger.valueOf(freq)
							.multiply(blockSize));
			
			numberOfComparisons = numberOfComparisons
					.add(BigInteger.valueOf(freq)
							.multiply(blockSize
									.multiply(blockSize
											.subtract(
													BigInteger.ONE))
									.shiftLeft(1)));
			
			CC = totalSizeOfBlocks.doubleValue()
					/ numberOfComparisons.doubleValue();
			
			Tuple2<Integer, Double> st = new Tuple2<Integer, Double>(
					blockSize.intValue(), CC);
			
			statistics.add(st);

		}

		int optimalBlockSize = statistics
				.get(statistics.size() - 1)._1;// lastBlockSize;

		double eps = 1d;

		/*
		 * find minimum difference for every adjacent pair i,i-1 the minimum
		 * difference represents the optimal blockSize
		 */
		for (int i = statistics.size() - 1; i >= 1; i--) {
			if (Math.abs(statistics.get(i)._2
					- statistics.get(i - 1)._2) < eps) {
				
				eps = Math.abs(statistics.get(i)._2
						- statistics.get(i - 1)._2);
				
				optimalBlockSize = statistics.get(i)._1;
			}
		}
		return optimalBlockSize;
	}

}

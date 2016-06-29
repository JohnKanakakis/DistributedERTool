package tests.statistics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import scala.Tuple2;

public class BlockPurgingOfficial {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		InputStream in = new FileInputStream(new File("block-test.txt"));
		String input = IOUtils.toString(in);
		Pattern pat = Pattern.compile("((\\d*),(\\d*))");
		Matcher matcher = pat.matcher(input);
		ArrayList<String> blockSizes = new ArrayList<String>();
		ArrayList<String> frequency = new ArrayList<String>();
		while(matcher.find()){
			blockSizes.add(matcher.group(2));
			frequency.add(matcher.group(3));
		}

		BigInteger numberOfComparisons = BigInteger.ZERO;
		BigInteger totalSizeOfBlocks = BigInteger.ZERO;
		BigInteger blockSize;
	
		double CC = 0d;

		CC = 0d;
		
		int lastBlockSize = 2;
		int f;
		
		List<Tuple2<Integer,Double>> statistics = new ArrayList<Tuple2<Integer,Double>>();
		
		
		for(int i = 0; i < blockSizes.size(); i++){
			blockSize = new BigInteger(blockSizes.get(i));
			f = Integer.parseInt(frequency.get(i));
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
		
		ArrayList<Tuple2<Integer,Double>> statistics1 = new ArrayList<Tuple2<Integer,Double>>(statistics);
		Collections.sort(statistics1,new Comparator<Tuple2<Integer,Double>>(){

			@Override
			public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
				// TODO Auto-generated method stub
				return Integer.compare(o1._1,o2._1);
			}
			
		});
		
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
	}

}

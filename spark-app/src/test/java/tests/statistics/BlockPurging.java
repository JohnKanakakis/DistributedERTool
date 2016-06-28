package tests.statistics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import scala.Tuple2;

public class BlockPurging {

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
		//System.out.println(blockSizes);
		//System.out.println(frequency);
		System.out.println(blockSizes.size() == frequency.size());
		double eps = 0.0000001;
		BigInteger numberOfComparisons = BigInteger.ZERO;
		BigInteger totalSizeOfBlocks = BigInteger.ZERO;
		BigInteger blockSize;
		BigInteger freq;
		double CC = 0d;
		double oldCC = 1d;
	
		int threshold = 0;
	
		for(int i = blockSizes.size()-1; i >= 0; i--){
			blockSize = new BigInteger(blockSizes.get(i));
			freq = new BigInteger(frequency.get(i));
			totalSizeOfBlocks = totalSizeOfBlocks.add(blockSize.multiply(freq));
			numberOfComparisons = numberOfComparisons.add(blockSize.multiply(freq).multiply(blockSize.multiply(freq).subtract(BigInteger.ONE)).shiftLeft(1));
			//System.out.println("total blocks = "+totalSizeOfBlocks);
			//System.out.println("total comp = "+numberOfComparisons);
			CC = totalSizeOfBlocks.doubleValue()/numberOfComparisons.doubleValue();
			//System.out.println("CC = "+CC);
			//System.out.println("oldCC = "+oldCC);
			if(Math.abs(CC - oldCC) <= eps){
				//System.out.println("diff "+Math.abs(CC - oldCC));
				threshold = blockSize.intValue();
				break;
			}
			oldCC = CC;
		}
		System.out.println(CC);
		System.out.println(threshold);
		
		CC = 0d;
		oldCC = 1d;
		int f;
		boolean threshold_reached = false;
		for(int i = blockSizes.size()-1; i >= 0; i--){
			if(threshold_reached) break;
			blockSize = new BigInteger(blockSizes.get(i));
			f = Integer.parseInt(frequency.get(i));
			for(int j = 0; j < f; j++){
				totalSizeOfBlocks = totalSizeOfBlocks.add(blockSize);
				numberOfComparisons = numberOfComparisons.add(blockSize.multiply(blockSize.subtract(BigInteger.ONE)).shiftLeft(1));
				//System.out.println("total blocks = "+totalSizeOfBlocks);
				//System.out.println("total comp = "+numberOfComparisons);
				CC = totalSizeOfBlocks.doubleValue()/numberOfComparisons.doubleValue();
				//System.out.println("CC = "+CC);
				//System.out.println("oldCC = "+oldCC);
				if(Math.abs(CC - oldCC) <= eps){
					System.out.println("diff "+Math.abs(CC - oldCC));
					threshold = blockSize.intValue();
					threshold_reached = true;
					break;
				}
				oldCC = CC;
			}
			
		}
		System.out.println(CC);
		System.out.println(threshold);
		
		
		
		
		
	}

}

package tests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aksw.limes.core.measures.measure.string.Jaro;

import org.apache.commons.math3.ml.clustering.CentroidCluster;

import scala.Tuple2;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;

public class DBScanTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String line = "2016-06-08 21:50:46,430 INFO  [Executor task launch worker-4] spark.Linker (Linker.java:call(255)) - block size = 15770";
		//String line = "2016-06-08 21:50:46,417 INFO  [Executor task launch worker-4] spark.Linker (Linker.java:call(257)) - mega block size = 30397for token = scattering";
		int p = line.indexOf("block size = ");
		line = line.substring(p);
		System.out.println(line);
		Pattern pat = Pattern.compile("block size = (\\d*)$");
		Matcher matcher = pat.matcher(line);
		if(matcher.find()){
			int x = Integer.parseInt(matcher.group(1));
			System.out.println(x);
		}
	}

	
	
	
}

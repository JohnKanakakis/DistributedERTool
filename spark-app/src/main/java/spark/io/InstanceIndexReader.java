package spark.io;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class InstanceIndexReader {

	/*public InstanceMapReader(SparkConf sc, JavaSparkContext c) {
		super(sc, c);
		// TODO Auto-generated constructor stub
	}*/


	public static JavaPairRDD<String, List<String>> run(JavaRDD<String> data) {
		// TODO Auto-generated method stub
		/*
		data = data.flatMap(new FlatMapFunction<String,String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String fileData) throws Exception {
				// TODO Auto-generated method stub
				String[] lines = fileData.split("\\n++");
				return Arrays.asList(lines);
			}
	    });*/
		
		JavaPairRDD<String,List<String>> instancesIndex = data.mapToPair(new PairFunction<String,String,List<String>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(String pair) throws Exception {
				
				//pair = pair.substring(1,pair.length()-1);
				//String[] tokens = line.split(" ++|\t++", 2);
				int pos = pair.indexOf(",");
				String key = pair.substring(1, pos);
				String value = pair.substring(pos+1);
				List<String> l = Arrays.asList(value.split(","));
				
				/*String[] tokens = line.split(",",2);
				
				if(tokens.length < 2 ){
					return new Tuple2<String,String>("","");
				}
				
				String subject = tokens[0];
				String values = tokens[1];
				*/
				
				return new Tuple2<String,List<String>>(key,l);
			}
		});
		
		return instancesIndex;
	}
    
	
}

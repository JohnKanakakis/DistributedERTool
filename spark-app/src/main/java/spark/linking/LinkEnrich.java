package spark.linking;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class LinkEnrich {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("LinkEnrich");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
	    JavaRDD<String> links = ctx.textFile(args[0]);
	    JavaRDD<String> timestampedLinks = links.mapPartitions(new FlatMapFunction<Iterator<String>,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Iterator<String> links) throws Exception {
				
				ArrayList<String> timestampedLinks = new ArrayList<String>();
				String link = null;
				
				while(links.hasNext()){
					link = links.next();
					timestampedLinks.add(link+" "+"oav:dateOfCollection: "+System.currentTimeMillis());
				}
				
				return timestampedLinks;
			}
	    	
	    });
	    timestampedLinks.saveAsTextFile(args[1]);
	    ctx.close();
	    
	}

}

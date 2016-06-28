package spark.differential;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class DifferentialModule {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("DifferentialModule");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    
	    SubjectToTriplePair sToTriplePair = new SubjectToTriplePair();
		
	    JavaRDD<String> links = ctx.textFile(args[0],10).filter(new Function<String,Boolean>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String triple) throws Exception {
				
				String[] t = triple.split(" ");
				int number = Integer.parseInt(t[0].substring(t[0].indexOf("_")+1));
				if(number % 100 == 0){
					return true;
				}
				return false;
				
			}
	    	
	    });
		JavaRDD<String> source = ctx.textFile(args[1],10);
	    links = links.coalesce(100);
	    source.cache();
	    links.cache();
	   
	    /*difference between source and links*/
	    sparkConf.log().info("so far so good");
	    DifferenceFunction<Iterator<Tuple2<String, String>>, String> df = 
	    		new DifferenceFunction<Iterator<Tuple2<String, String>>,String>();
	    
	    
	   	int numP = links.partitions().size();
	   	int[] p = new int[1];
		
		p[0] = 0;
		List<String>linksP = links.collectPartitions(p)[0];
		Broadcast<List<String>> linksB = ctx.broadcast(linksP);
		df.setLinks(linksB);
		JavaPairRDD<String,String> diff = source.mapPartitions(df).mapToPair(sToTriplePair);
		JavaPairRDD<String, String> inters = diff;
	    
	   	inters.cache();
	   	diff.cache();
	   	sparkConf.log().info("so far so good 1");
		for(int i = 1; i < numP; i++){
			p[0] = i;
			linksP = links.collectPartitions(p)[0];
			linksB = ctx.broadcast(linksP);
			df.setLinks(linksB);
			diff = source.mapPartitions(df).mapToPair(sToTriplePair);
			inters = inters.intersection(diff);
		}
	   	inters.map(new Function<Tuple2<String, String>, String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> t) throws Exception {
				// TODO Auto-generated method stub
				if (t == null) return null;
				return t._2;
			}
	   		
	   	}).coalesce(10).saveAsTextFile(args[2]);
		ctx.close();
	}

}

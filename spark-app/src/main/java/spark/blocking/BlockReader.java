package spark.blocking;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class BlockReader {

	public static JavaPairRDD<String, Tuple2<String, String>> run(JavaRDD<String> data) {
		// TODO Auto-generated method stub
	   
		JavaPairRDD<String, Tuple2<String, String>> blocks = data.mapToPair(new PairFunction<String,String,Tuple2<String, String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String,Tuple2<String, String>> call(String pair) throws Exception {
				// TODO Auto-generated method stub
				//System.out.println(pair);
				int pos = pair.indexOf(",");
				String key = pair.substring(1,pos);
				String value = pair.substring(pos+1,pair.length()-1);
				
				int pos1 = value.indexOf(",");
				if(pos1 < 0){
					System.out.println("value|"+value);
				}
				String key1 = value.substring(1,pos1);
				String value1 = value.substring(pos1+1,value.length()-1);
				
				Tuple2<String,String> t = new Tuple2<String,String>(key1,value1);
				
				return new Tuple2<String,Tuple2<String,String>>(key,t);
			}
	    });
	    
	    return blocks;
	}

}

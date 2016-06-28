package spark.filter;



import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class DataFilter {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("DataFilter");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
	    JavaRDD<String> fullTriples = ctx.textFile(args[0]);
		JavaRDD<String> filteredTriples = fullTriples.filter(new Function<String,Boolean>(){

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
		filteredTriples.saveAsTextFile(args[1]);
		
		
		ctx.close();
	}

}

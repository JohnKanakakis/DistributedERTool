package spark.help;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class RepartitionFile {

	public final static org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("Repartition");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	int partitions = Integer.parseInt(args[2]);
    	ctx.textFile(args[0]).repartition(partitions).saveAsTextFile(args[1]);
    	ctx.close();
	}

}

package spark.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class DataReader {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("DataReaderAndFilter");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	String resourceType = args[0];
    	
    	Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("textinputformat.record.delimiter", "\n");
        
        JavaRDD<String> records = DataReader.runWithResourceTypeFilter(ctx.newAPIHadoopFile(args[1], 
        											 TextInputFormat.class, 
        											 LongWritable.class, 
        											 Text.class,
        											 conf),resourceType).repartition(800);
        
    	records.saveAsTextFile(args[2]);
    	ctx.close();
	}
	
	
	public static JavaRDD<String> run(JavaPairRDD<LongWritable,Text> data){
		
        JavaRDD<String> lines = data
        							.map(new Function<Tuple2<LongWritable,Text>,String>(){
										private static final long serialVersionUID = 1L;
										@Override
										public String call(Tuple2<LongWritable, Text> t) 
												throws Exception {
											// TODO Auto-generated method stub
											return t._2.toString();
										}
    								});
        return lines;
	}

	
	public static JavaRDD<String> runWithResourceTypeFilter(JavaPairRDD<LongWritable,Text> data,final String resourceType){
		JavaRDD<String> lines = data
				.filter(new Function<Tuple2<LongWritable,Text>,Boolean>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Boolean call(Tuple2<LongWritable, Text> t) 
							throws Exception {
						// TODO Auto-generated method stub
						if(t._2.toString().contains(resourceType))
							return true;
						
						return false;
					}
				}).map(new Function<Tuple2<LongWritable,Text>,String>(){
					private static final long serialVersionUID = 1L;
					@Override
					public String call(Tuple2<LongWritable, Text> t) 
							throws Exception {
						// TODO Auto-generated method stub
						return t._2.toString();
					}
				});
		return lines;
	}
}

package spark.creation;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class DataCreation {

	
	static String SUBJECT = "http://example.com/John_";
	static String PREDICATE = "http://example.com/works";
	static String OBJECT = "http://example.com/ntua_";
	static String SPACE = " ";
	static String DATE_OF_COLLECTION = "oav:dateOfCollection: ";
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String outputFile = args[0];
		int numberOfTriples = Integer.parseInt(args[1]);
		int numberOfPartitions = Integer.parseInt(args[2]);
		
		SparkConf sparkConf = new SparkConf().setAppName("DataCreation");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		/*
	    Class<?>[] classesForSer = new Class<?>[1];
		classesForSer[0] = CreateDataFunction.class;
		
		sparkConf.registerKryoClasses(classesForSer);
		
		sparkConf.set("spark.kryo.registrationRequired","true");
		sparkConf.set("spark.kryoserializer.buffer.max","512m");*/
	    
	    
	    /*AccumulableParam<ArrayList<String>, String> accumulatorParam = new LinkAccumulableParam();
	    Accumulable<ArrayList<String>, String> acclinks;
	    acclinks = ctx.accumulable(new ArrayList<String>(), "resources",accumulatorParam);
		acclinks.setValue(new ArrayList<String>());*/
	    
	    List<Integer>  t = new ArrayList<Integer>();
	    
	    for(int i = 0; i < numberOfPartitions; i++){
	    	t.add(i);
	    }
	    
	    JavaRDD<Integer> triples = ctx.parallelize(t,numberOfPartitions);
	    CreateDataFunction<String,Integer> create = new CreateDataFunction<String,Integer>();
	    create.setFractionOfTriples(numberOfTriples/numberOfPartitions);
	    
	    JavaRDD<String> fullTriples = triples.flatMap(create);
	    
	    
	    
	    
	    
	    fullTriples.mapToPair(new PairFunction<String,String,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String triple) throws Exception {
				// TODO Auto-generated method stub
				String t[] = triple.split(" ");
				
				return new Tuple2<String, String>(t[0],t[1]+"###"+t[2]);
			}
	    	
	    }).reduceByKey(new Function2<String,String,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1, String v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+"???"+v2;
			}
	    	
	    }).coalesce(10).saveAsTextFile(outputFile);
	    ctx.close();
	    
		/*if (args.length < 1) {
		      System.err.println("Usage: JavaWordCount <file>");
		      System.exit(1);
		    }

		    SparkConf sparkConf = new SparkConf().setAppName("DataCreation");
		    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		    
		    //JavaRDD<String> lines = ctx.textFile(args[0], 1);

		    
		    try {
		    	Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				
				Path outputFile = new Path(args[0]);
				if (fs.exists(outputFile)){
					fs.delete(outputFile, false);
				}
				FSDataOutputStream out = fs.create(outputFile);
				
				for(int i = 0 ; i < Integer.parseInt(args[1]); i++){
					out.writeBytes(SUBJECT + i+SPACE + 
								   PREDICATE + SPACE + 
								   OBJECT + +i+SPACE + 
								   DATE_OF_COLLECTION+System.currentTimeMillis()+"\n");
			    }
			    out.close();
			    ctx.close();
			    
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		    
		    
		    /*Broadcast<String[]> tripleVar = ctx.broadcast(triple);
		    
		    Accumulator<Integer> numberOfTriples = ctx.accumulator(0);
		    
		    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterable<String> call(String s) {
		        return Arrays.asList(SPACE.split(s));
		      }
		    });

		    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
		      @Override
		      public Tuple2<String, Integer> call(String s) {
		        return new Tuple2<String, Integer>(s, 1);
		      }
		    });

		    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
		      @Override
		      public Integer call(Integer i1, Integer i2) {
		        return i1 + i2;
		      }
		    });*/
		
	}

	
	public static void run(int numberOfTriples, String outputFile){
		
		
	}

	
}

package spark.linking;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;

import scala.Tuple2;

public class DataLink {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("DataLinking");
		Logger logger = sparkConf.log();
		
		
		Class<?>[] classesForSer = new Class<?>[4];
		classesForSer[0] = LinkFunction.class;
		classesForSer[1] = Iterator.class;
		classesForSer[2] = String[].class;
		
		classesForSer[3] = ArrayList.class;
		sparkConf.registerKryoClasses(classesForSer);
		
		sparkConf.set("spark.kryo.registrationRequired","true");
		sparkConf.set("spark.kryoserializer.buffer.max","512m");
	    

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		String sourceFile = args[0];
		String targetFile = args[1];
		String configFile = args[2];
		String linksFile = args[3];
		
		
	    JavaRDD<String> source = ctx.textFile(sourceFile, 4);
	    JavaRDD<String> target = ctx.textFile(targetFile, 4);
	    source.cache();
	    //target.cache();
	    
	    
	    Broadcast<String> confB = ctx.broadcast(configFile);
	   
	   
	    org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = null;
		/*try {
			hdfs = FileSystem.get(hdfsConf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Broadcast<List<String>> targetB = null;
		try { 
			InputStream in = hdfs.open(new Path(targetFile)).getWrappedStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			
			List<String> lines = new ArrayList<String>();
			String line = br.readLine();
			lines.add(line);
			while(line != null){
				line = br.readLine();
				lines.add(line);
			}
			//String[] ls = new String[lines.size()];
			//ls = lines.toArray(ls);
			//sparkConf.log().info(ls[0]);
			//sparkConf.log().info(ls[1]);
			//sparkConf.log().info(ls[2]);
			//sparkConf.log().info(ls[3]);
			targetB = ctx.broadcast(lines);
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
		//LinkFunction<String, String> linkfunction = new LinkFunction<String,String>();
	    //linkfunction.setConfiguration(confB);
		VoidLinkFunction voidLinkFunction = new VoidLinkFunction();
	    voidLinkFunction.setConfiguration(confB);
	    
	    
		AccumulableParam<ArrayList<String>, String> accumulatorParam = new LinkAccumulableParam();
		Accumulable<ArrayList<String>, String> acclinks = ctx.accumulable(new ArrayList<String>(), "links",accumulatorParam);
		acclinks.setValue(new ArrayList<String>());
		
		
		int tPartitions = target.partitions().size();
		int[] p = new int[1];
		for(int i = 0; i < tPartitions; i++){
			p[0] = i;
			List<String> targetP = target.collectPartitions(p)[0];
			logger.info("broadcasting target partition "+i);
			Broadcast<List<String>> targetB = ctx.broadcast(targetP);
			voidLinkFunction.setTarget(targetB);
			voidLinkFunction.setAccLink(acclinks);
			//JavaRDD<String> links = source.mapPartitions(linkfunction);
			source.foreachPartition(voidLinkFunction);
		}
		acclinks.value();
		//Broadcast<List<String>> targetB = ctx.broadcast(target.collect());
	    
		

	    //JavaRDD<String> links = source.mapPartitions(linkfunction);
	    
	    /*VoidLinkFunction voidLinkFunction = new VoidLinkFunction();
	    voidLinkFunction.setTarget(targetB);
	    voidLinkFunction.setConfiguration(confB);
	    
	    source.foreachPartition(voidLinkFunction);*/
	    
	    
	    
	    
		/*try {
			
			hdfs = FileSystem.get(hdfsConf);
			Path linksFilePath = new Path(linksFile);
			if (hdfs.exists(linksFilePath)){
				hdfs.delete(linksFilePath, true);
			}
			acclinks.saveAsTextFile(linksFilePath.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

}

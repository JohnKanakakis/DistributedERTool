package spark;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.io.DataReader;

public class ZipExtractor {
	public static Logger logger = LoggerFactory.getLogger(ZipExtractor.class);
	public final static org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
	public static FileSystem hdfs;
	
	public static void main(String[] args){
		
		SparkConf sparkConf = new SparkConf().setAppName("ZipExtractor");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		JavaPairRDD<String, PortableDataStream> zipFiles = ctx.binaryFiles(args[0]);
    	final String unzippedDir = args[1];
    	
    	
    	
    	zipFiles.foreach(new VoidFunction<Tuple2<String,PortableDataStream>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, PortableDataStream> zipFile) throws Exception {
				// TODO Auto-generated method stub
				logger.info("reading file "+zipFile._1);
				DataInputStream stream = zipFile._2.open();
				
				GZIPInputStream zis = new GZIPInputStream(stream);
				
				byte[] buffer = new byte[1024];
				hdfs = FileSystem.get(hdfsConf);
				///user/kanakakis/openaire/OA_dump_000002.nt.gz
				
				String fileName = zipFile._1.substring(zipFile._1.lastIndexOf("/"));
				fileName = fileName.substring(0,fileName.lastIndexOf("."));
				FSDataOutputStream out = hdfs.create(new Path(unzippedDir+"/"+fileName), true, 1024);
		        int len;
		        while ((len = zis.read(buffer)) > 0) {
		        	out.write(buffer, 0, len);
		        }
		        out.close();
		        zis.close();
		        stream.close();
		      
		        hdfs.close();
		       // zis = null;
		        //stream = null;
			}
    	});
    	
		//ctx.textFile(unzippedDir,500).saveAsTextFile(unzippedDir+"_new");
    	ctx.close();
		
	}
	
	
	
	
	
	
	
	
	
	public static void main_old(String[] args) {
		// TODO Auto-generated method stub

		
		FileSystem hdfs = null;
    	List<Tuple2<String,String>> IOFilePairs= new ArrayList<Tuple2<String,String>>();
    	Path input = new Path(args[0]);
    	String outputDirectory = args[1];
		
		logger.info("so far so good");
		logger.info(":"+args[0]);
		
		
		
		
		try {
			hdfs = FileSystem.get(hdfsConf);
			RemoteIterator<LocatedFileStatus> filesIterator = hdfs.listFiles(input, false);
			Tuple2<String,String> ioPair;
			
			
			String inputFile;
			String fileID;
			String fileName;
			Pattern p = Pattern.compile("(.*).gz");
			
			while(filesIterator.hasNext()){
				LocatedFileStatus fileStatus = filesIterator.next();
				inputFile = fileStatus.getPath().toString();
				fileName = fileStatus.getPath().getName().toString();
				Matcher matcher = p.matcher(fileName);
				matcher.find();
				fileID = matcher.group(1);
				ioPair = new Tuple2<String,String>(inputFile,
												   outputDirectory+"/"+fileID);
				IOFilePairs.add(ioPair);
				
			}
			logger.info(IOFilePairs.toString());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			
			try {
				hdfs.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				//ctx.close();
			}
			
			e.printStackTrace();
			//ctx.close();
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("ZipExtractor");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<Tuple2<String,String>> files = ctx.parallelize(IOFilePairs,800);
		
		try {
			unzip(files);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			ctx.close();
		}
		
		ctx.close();
		//SparkConf sparkConf = new SparkConf().setAppName("Controller");
		
    	//JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
    	//JavaRDD<String> data = ctx.textFile(args[0]);
    	
		/*data.mapToPair(new PairFunction<String,String,List<String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(String record) throws Exception {
				// TODO Auto-generated method stub
				List<String> triple = DataFormatter.toTriple(record);
				if(triple == null){
					return null;
				}
				String r_id = triple.get(0);
				triple.remove(0);
				return new Tuple2<String,List<String>>(r_id,triple);
			}
    	}).reduceByKey(new Function2<List<String>,List<String>,List<String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(List<String> l1, List<String> l2) throws Exception {
				// TODO Auto-generated method stub
				l1.addAll(l2);
				return l1;
			}
    	}).saveAsTextFile(args[1]);
		
		ctx.close();*/
	}

	private static void unzip(JavaRDD<Tuple2<String,String>> files) throws IOException{
		
		files.foreach(
        		new VoidFunction<Tuple2<String,String>>(){
    				private static final long serialVersionUID = 1L;
    				
					@Override
					public void call(Tuple2<String,String> ioFilePair) throws Exception {
						InputStream in = null;
						try {
							String inputFile = ioFilePair._1;
							String outputFile = ioFilePair._2;
							byte[] buffer = new byte[1024];
							FileSystem hdfs = FileSystem.get(hdfsConf);
							
							in = hdfs.open(new Path(inputFile));
							GZIPInputStream zis = new GZIPInputStream(in);
							FSDataOutputStream out = hdfs.create(new Path(outputFile), true, 1024);
							
					        int len;
					        while ((len = zis.read(buffer)) > 0) {
					        	out.write(buffer, 0, len);
					        }
					 
					        zis.close();
					    	out.close();
						 
							
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					}
        		});
		
		//org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		//FileSystem hdfs = null;
		//String fileContent = "";
		
	}
}

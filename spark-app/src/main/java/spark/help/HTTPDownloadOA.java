package spark.help;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPDownloadOA {

	public static Logger logger = LoggerFactory.getLogger(HTTPDownloadOA.class);
	public final static org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
	public static FileSystem hdfs;
	public final static String BASE_URL = "https://zenodo.org/record/53077/files/";
	public final static int N = 122;
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf sparkConf = new SparkConf().setAppName("HTTPDownloadOA");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
    	ArrayList<String> fileURLs = new ArrayList<String>();
    	final String outputDir = args[0];
    	
    	for(int i = 2; i <= N; i++){
			
    		fileURLs.add("OA_dump_"+String.format("%06d", i)+".nt.gz");
				
		}
    	JavaRDD<String> fileURLsRDD = ctx.parallelize(fileURLs);
    	
    	
    	fileURLsRDD.foreach(new VoidFunction<String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String fileURL) throws Exception {
				// TODO Auto-generated method stub
				logger.info("downloading file "+fileURL);
				hdfs = FileSystem.get(hdfsConf);
				
			    URL url = new URL(BASE_URL+fileURL);
			    
		        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		        InputStream in = null;
		        
		        try{
		        	in = httpConn.getInputStream();
		        	FSDataOutputStream out = hdfs.create(new Path(outputDir+"/"+fileURL),true);
			        int bytesRead = -1;
		            byte[] buffer = new byte[4096];
		            while ((bytesRead = in.read(buffer)) != -1) {
		                out.write(buffer, 0, bytesRead);
		            }
		            logger.info(fileURL+" written to hdfs");
		            out.close();
		            in.close();
		           
		        }catch(IOException e){
		        	logger.error(fileURL +" does not exist");
		        	//in.close();
		        	//out.close();
		        }
		      
				
			}
    		
    	});
    	ctx.close();
		
	}

}

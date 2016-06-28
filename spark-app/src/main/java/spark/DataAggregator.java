package spark;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class DataAggregator {

	public static void run(JavaRDD<String> files,String outputDirectory){
		
		
		/*String base = inputDirectory;
		List<String> filesInDirectory = new ArrayList<String>();
        for(int i = 0; i < 197; i++){
        	if(i < 10)
        		filesInDirectory.add(base+"/part-m-0000"+i+".ttl");
        	else if(i < 100)
        		filesInDirectory.add(base+"/part-m-000"+i+".ttl");
        	else
        		filesInDirectory.add(base+"/part-m-00"+i+".ttl");
        }
        JavaRDD<String> files = ctx.parallelize(filesInDirectory,10);*/
        
        
        JavaRDD<String> data1 = files.map(new Function<String,String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String file) throws Exception {
				// TODO Auto-generated method stub
				org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
				FileSystem hdfs = null;
				String data = "";
				try {
					hdfs = FileSystem.get(hdfsConf);
					InputStream in = hdfs.open(new Path(file)).getWrappedStream();
					data = IOUtils.toString(in);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				return data;
			}
        });

        data1.saveAsTextFile(outputDirectory);
	}
}

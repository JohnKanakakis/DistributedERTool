package spark.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;




public class InstanceIndexCreator {

	public static JavaPairRDD<String,List<String>> runWithoutFormatting(JavaPairRDD<String, List<String>> data1) {
		
		/*JavaPairRDD<String,List<String>> instances = data1.mapToPair(new PairFunction<String,String,List<String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(String record) throws Exception {
				// TODO Auto-generated method stub
				
				//record = (p_id,[info])
				
				
				int pos = triple.indexOf("\n");
				String subject = triple.substring(0, pos);
				
				
				
				if(p_id == null){
					return new Tuple2<String,List<String>>("",new ArrayList<String>());
				}
				return new Tuple2<String,List<String>>(p_id,infoAsList);
			}
	    });
		
		return instances;*/
		return null;
	}
	
	public static JavaPairRDD<String,String> run(JavaRDD<String> data) {
		
	    JavaPairRDD<String,String> instances = data.flatMapToPair(new PairFlatMapFunction<String,String,String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(String line) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<String,String>> instanceMap = new ArrayList<Tuple2<String,String>>();
				
				String[] POs = null;
				String subject = null;
				String firstPO = null;
				String subjectMap = null;
				String[] splitPO = null;

				
				POs = line.split("\\;\\n\\t++");
				
				if(POs.length < 2){
					//logger.error(line);
					
					return new ArrayList<Tuple2<String,String>>();
				}
				String[] s = POs[0].split("\\n\\t++");
				if(s.length < 2){
					
					return new ArrayList<Tuple2<String,String>>();
				}
				subject = POs[0].split("\\n\\t++")[0];
				firstPO = POs[0].split("\\n\\t++")[1];
				
				splitPO = firstPO.replace("\"", "'").split(" ",2);
				subjectMap = "{\""+splitPO[0]+"\":\""+splitPO[1]+"\"}";
				instanceMap.add(new Tuple2<String,String>(subject,subjectMap));
				
				for(int j = 1; j < POs.length;j++){
					splitPO = POs[j].replace("\"", "'").split(" ",2);
					subjectMap=("{\""+splitPO[0]+"\":\""+splitPO[1]+"\"}");
					instanceMap.add(new Tuple2<String,String>(subject,subjectMap));
				}
				return instanceMap;
			}
	    	
	    }).reduceByKey(new Function2<String,String,String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1, String v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+","+v2;
			}
	    });
	    
	    return instances;
	    /*
	     ArrayList<Tuple2<String,String>> instanceMap = new ArrayList<Tuple2<String,String>>();
				
				String[] POs = null;
				String subject = null;
				String firstPO = null;
				String subjectMap = null;
				String[] splitPO = null;

				for(int i = 0; i < lines.length;i++){
					if(lines[i].startsWith("@")) continue;
					
					POs = lines[i].split("\\;\\n\\t++");
					
					subject = POs[0].split("\\n\\t++")[0];
					firstPO = POs[0].split("\\n\\t++")[1];
					
					splitPO = firstPO.replace("\"", "'").split(" ",2);
					
					
					subjectMap = "{\"values\":"+"["+"{\""+splitPO[0]+"\":\""+splitPO[1]+"\"}";
					for(int j = 1; j < POs.length;j++){
						
						splitPO = POs[j].replace("\"", "'").split(" ",2);
						subjectMap+=(","+"{\""+splitPO[0]+"\":\""+splitPO[1]+"\"}");
					}
					subjectMap+=("]}");
					instanceMap.add(new Tuple2<String,String>(subject,subjectMap));
				}
				return instanceMap;
				
	     */
	   
	   
	  	//instances.saveAsTextFile(args[1]);
	    
	}

	

}

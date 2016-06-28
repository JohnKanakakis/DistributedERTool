package spark.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.blocking.BlockCreator;
import spark.model.DatasetManager;

public class DataFormatter {

	public static final String SEPERATOR = "@@@";
	public static Logger logger = LoggerFactory.getLogger(DataFormatter.class);
	public static JavaRDD<String> run(JavaRDD<String> data){
		
		data = data.filter(new Function<String,Boolean>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String line) throws Exception {
				if(line.startsWith("@")) return false;
				if(line.startsWith("\\n")) return false;
				return true;
			}
	    });
		data = data.map(new Function<String,String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public String call(String line) throws Exception {
				Pattern emptyPtrn = Pattern.compile("(\\n*)(.*)",Pattern.DOTALL);
				Matcher matcher = emptyPtrn.matcher(line);
				matcher.find();
				
				String triple = matcher.group(2);
				if(triple != null){
					return triple+".";
				}
				return "";
			}
	    });
		return data;
	}

	
	public static JavaPairRDD<String, List<String>> formatToPair_String_ArrayList(JavaRDD<String> records,
																				  final String datasetID) {
		// TODO Auto-generated method stub
		return records.mapToPair(new PairFunction<String,String,List<String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(String record) throws Exception {
				// TODO Auto-generated method stub
				
				//record = (p_id,[info])
				
				int pos = record.indexOf(",");
				String r_id = record.substring(1, pos);
				r_id = DatasetManager.addDatasetIdToResource(r_id,datasetID);
				record = record.replace(", ", ",");
				String info = record.substring(pos+2,record.length()-2);
				
				String[] l = info.split("@@@,|@@@");
				ArrayList<String> infoAsList = new ArrayList<String>();
				
				if(l.length%2 != 0){
					logger.error("malformed list ");
					for(int i = 0; i < l.length;i++){
						System.out.println(l[i]);
					}
					if(l[0].equals(l[1])){
						logger.error("first two elements are the same");
					}else{
						logger.error("could not corrected list..exiting");
						
					}
					return null;
				}
				//System.out.println(infoAsList);
				for(int i = 0; i < l.length; i++){
					infoAsList.add(l[i]);
				}
					
				return new Tuple2<String,List<String>>(r_id,infoAsList);
			}
		});
		
		
		
		
		//Partitioner partitioner = Partitioner.;
		
		/*return records.keyBy(new Function<String,String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public String call(String record) throws Exception {
				// TODO Auto-generated method stub
				int pos = record.indexOf(",");
				String p_id = record.substring(1, pos)+"_d"+datasetID;
				return p_id;
			}
		}).mapValues(new Function<String,List<String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(String value) throws Exception {
				// TODO Auto-generated method stub
				
				//record = (p_id,[info])
				//value = (p_id,[info])
				
				int pos = value.indexOf(",");
				//String p_id = value.substring(1, pos);
				value = value.replace(", ", ",");
				String info = value.substring(pos+2,value.length()-2);
				
				ArrayList<String> infoAsList = new ArrayList<String>(Arrays.asList(info.split(",")));
				//System.out.println(infoAsList);
				return infoAsList;//new Tuple2<String,List<String>>(p_id,infoAsList);
			}
		});*/
	}

	public static String getResourceId(String record){
		
		int pos = record.indexOf(",");
		String r_id = record.substring(1, pos);
		return r_id;
	}
	
	public static String[] getResourceInfo(String record){
		int pos = record.indexOf(",");
		record = record.replace(", ", ",");
		String info = record.substring(pos+2,record.length()-2);
		
		return info.split("@@@,|@@@");
	}
	
	public static String eliminateDataTypeFromLiteral(String literal){
		int pos = literal.lastIndexOf("^^");
		if(pos == -1){
			return literal;
		}
		return literal.substring(0,pos);
	}
	
	public static List<String> toTriple(String s,List<String> l){
		
		int pos1;
		int pos2;
		String p;
		int cnt = 0;
		//ArrayList<String> triple = new ArrayList<String>();
		boolean validTriple = true;
		while(validTriple){
			pos1 = s.indexOf("<");
			pos2 = s.indexOf(">");
			if(pos1 == -1 || pos2 == -1 || pos2 < pos1){
				if(cnt == 3){
					//System.out.println("valid triple..exiting");
					break;
				}else{
					//System.out.println("invalid triple..exiting");
					pos1 = s.indexOf("\"");
					if(pos1 == -1){
						validTriple = false;
					}else{
						pos2 = s.lastIndexOf('\"');
						p = s.substring(pos1,pos2+1);
						l.add(p);
						//validTriple = true;
						break;
					}
				}
			}else{
				p = s.substring(pos1+1,pos2);
				l.add(p);
				s = s.substring(pos2+1);
				cnt++;
			}
		}
		if(!validTriple){
			/*for(int i = 0; i < l.size(); i++)
				l.set(i, "");*/
			return null;
		}
		return l;
	}
	
	/*public static JavaPairRDD<String, Set<String>> formatToPair_String_Set(JavaRDD<String> records) {
		// TODO Auto-generated method stub
		return records.mapToPair(new PairFunction<String,String,Set<String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Set<String>> call(String record) throws Exception {
				// TODO Auto-generated method stub
				
				//record = (p_id,[info])
				
				int pos = record.indexOf(",");
				String p_id = record.substring(1, pos);
				record = record.replace(", ", ",");
				String info = record.substring(pos+2,record.length()-2);
				String ar[] = info.split(",");
				HashSet<String> set = new HashSet<String>();
				for(int i = 0; i < ar.length; i++){
					set.add(ar[i]);
				}
				//System.out.println(infoAsList);
				return new Tuple2<String,Set<String>>(p_id,set);
			}
			
		});
		
	}*/
}

/*class AddDatasetIdFunc {
	
	static <V> JavaPairRDD<String, V> run(JavaPairRDD<String, V> data, final Integer datasetID){
		
		return data.mapToPair(new PairFunction<Tuple2<String,V>,String,V>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, V> call(Tuple2<String,V> record) throws Exception {
				// TODO Auto-generated method stub
				String subject = record._1;
				return new Tuple2<String,V>(subject+"_d"+datasetID,record._2);
			}
			
		});
	}
};*/

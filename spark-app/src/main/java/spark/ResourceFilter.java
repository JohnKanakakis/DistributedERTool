package spark;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aksw.limes.core.io.config.KBInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.vocabulary.RDF;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashBigSet;
import scala.Tuple2;
import spark.blocking.BlockCreator;
import spark.filter.DataFormatter;
import spark.filter.PersonFilter;
import spark.io.DataReader;
import spark.io.InstanceIndexCreator;
import spark.io.InstanceIndexReader;
import spark.model.DatasetInfo;
import spark.model.DatasetManager;


public class ResourceFilter {
	public static Logger logger = LoggerFactory.getLogger(ResourceFilter.class);
	public final static Property TYPE_PROPERTY = RDF.type;
	
    public static void main(String[] args) {
    	
    	SparkConf sparkConf = new SparkConf().setAppName("Controller");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	Logger logger = sparkConf.log();
       
    	
    	Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("textinputformat.record.delimiter", ")\n");
        
        JavaRDD<String> data1 = DataReader.run(ctx.newAPIHadoopFile(args[0], 
        											 TextInputFormat.class, 
        											 LongWritable.class, 
        											 Text.class,
        											 conf));
        							
        JavaRDD<String> data2 = DataReader.run(ctx.newAPIHadoopFile(args[0], 
			 										TextInputFormat.class, 
	 												LongWritable.class, 
	 												Text.class,
	 												conf)); 
        
    	ctx.close();
    }
    
    /*public static JavaPairRDD<String, List<String>> run(JavaRDD<String> records,
    													final DatasetInfo datasetInfo)
    {
    	
    	PairFlatMapFunction<Iterator<String>, String, List<String>> f = 
    			new PairFlatMapFunction<Iterator<String>,String,List<String>>(){
					private static final long serialVersionUID = 1L;
		
					@Override
					public Iterable<Tuple2<String, List<String>>> call(Iterator<String> records) 
					throws Exception {
						// TODO Auto-generated method stub
						HashSet<Tuple2<String,List<String>>> resources = 
								new HashSet<Tuple2<String,List<String>>>();
						
						String record;
						String r_id;
						String[] r_info;
						List<String> infoToKeep;
						
						String resourceClass = datasetInfo.getTypeClass();
						HashSet<String> properties = datasetInfo.getProperties();
						String datasetId = datasetInfo.getId();
						
						while(records.hasNext()){
							record = records.next();
							r_id = DataFormatter.getResourceId(record);
							r_info = DataFormatter.getResourceInfo(record);
							if(belongsToResourceClass(r_info,resourceClass)){
								infoToKeep = filterResourceInfo(r_info,properties);
								if(infoToKeep == null) continue;
								r_id = DatasetManager.addDatasetIdToResource(r_id, datasetId);
								resources.add(new Tuple2<String,List<String>>(r_id,infoToKeep));
							}
						}
						
						return resources;
					}
    		
    	};
    	return records.mapPartitionsToPair(f);
    }*/
    
    /*private static boolean belongsToResourceClass(String[] info, String resourceClass){
    	
    	for(int i = 0; i < info.length -1; i = i + 2){
    		if(info[i].equals(TYPE_PROPERTY.toString())){
    			if(info[i+1].equals(resourceClass))
    				return true;
    			else
    				return false;
    		}
    	}
    	return false;
    }*/
    
    private static boolean hasCorrectProperties(Set<Tuple2<String, String>> resourceProperties, HashSet<String> configProperties){
    	

    	//boolean hasCorrectProperties = false;
    	
    	HashSet<String> resourcePropertiesNames = new HashSet<String>();
    	for(Tuple2<String, String> po : resourceProperties)
    		resourcePropertiesNames.add(po._1);
    	
    	if(resourcePropertiesNames.containsAll(configProperties))
    		return true;
    	/*for(Tuple2<String, String> t : info){
    		if(properties.contains(t._1)){
    			hasCorrectProperties = false;
    		}
    	}*/
    	return false;
    }
    
    private static List<String> filterResourceInfo(Set<Tuple2<String, String>> pos, HashSet<String> configProperties){
    	
    	ArrayList<String> infoTokeep = new ArrayList<String>();
    	for(Tuple2<String, String> po : pos){
    		if(configProperties.contains(po._1)){
    			infoTokeep.add(po._1);
    			infoTokeep.add(po._2);
    		}
    	}
    	if(infoTokeep.size() == 2)
    		return null;
    	return infoTokeep;
    }
    
    /*private static List<String> filterResourceInfo(String[] info, Set<String> properties){
    	
    	
    	properties.add(TYPE_PROPERTY.toString());
    
    	ArrayList<String> infoTokeep = new ArrayList<String>();
    	for(int i = 0; i < info.length - 1; i = i + 2){
    		if(properties.contains(info[i])){
    			infoTokeep.add(info[i]);
    			infoTokeep.add(info[i+1]);
    		}
    	}
    	if(infoTokeep.size() == 2)
    		return null;
    	return infoTokeep;
    }*/
    
    /*public static JavaPairRDD<String, List<String>> run(JavaPairRDD<String, List<String>> data,
    		                                            final String resourceClass,
    		                                            final List<String> properties)
    {
    	
    	return data.filter(new Function<Tuple2<String,List<String>>,Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String,List<String>> record) throws Exception {
				// TODO Auto-generated method stub
				String p1 = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
				String o1 = "http://xmlns.com/foaf/0.1/Person";
				
				String p2 = "http://xmlns.com/foaf/0.1/firstName";
				String p3 = "http://xmlns.com/foaf/0.1/lastName";
				
				List<String> pos = record._2;
				
				int index = pos.indexOf(TYPE_PROPERTY.toString());
				
				if(index == -1){
					//System.out.println("malformed pos list"+pos);
					return false;
				}
				
				if(pos.get(index+1).equals(resourceClass)){
					if(pos.contains(p2) && pos.contains(p3))
						return true;
					else
						return false;
				}
				
				if(index == pos.size()-1){
					//System.out.println("malformed pos list"+pos);
					return false;
				}
				return true;
			}
		});
    }*/

	public static JavaPairRDD<String, List<String>> runWithPairs(
			JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> records, final Broadcast<byte[]> kbB) {
		// TODO Auto-generated method stub
		final KBInfo kb = (KBInfo)Utils.deserialize(kbB.getValue());
		
		final HashSet<String> configProperties = new HashSet<String>(kb.getProperties());
		configProperties.add(TYPE_PROPERTY.toString());
		
		JavaPairRDD<String, List<String>> result = 
		records.filter(new Function<Tuple2<String, Set<Tuple2<String, String>>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, String>>> t) throws Exception {
				// TODO Auto-generated method stub
			
				boolean hasTypeClass = false;
				for(Tuple2<String,String> po : t._2){
					if(po._1.equals(TYPE_PROPERTY.toString()) && po._2.equals(kb.getClassRestriction())){
						hasTypeClass = true;
					}
				}
				if(hasTypeClass){
					if(hasCorrectProperties(t._2,configProperties)){
					//logger.info("accepting tuple "+t);
						return true;
					};
				}
				
				return false;
			}
			
		}).mapToPair(new PairFunction<Tuple2<String, Set<Tuple2<String, String>>>,String,List<String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(Tuple2<String, Set<Tuple2<String, String>>> resource)
					throws Exception {
				// TODO Auto-generated method stub
				/*List<String> infoToKeep = new ArrayList<String>();
		    	for(Tuple2<String, String> t : resource._2){
		    		
		    		infoToKeep.add(t._1);
		    		infoToKeep.add(t._2);
		    		
		    	}*/
				List<String> infoToKeep = filterResourceInfo(resource._2,configProperties);
				if(infoToKeep == null){
					logger.error("accepted false tuple "+resource);
					return null;
				}
				String r_id = DatasetManager.addDatasetIdToResource(resource._1, kb.getId());
				
				return new Tuple2<String,List<String>>(r_id,infoToKeep);
				
				
			}
		});
		return result;
	}


    
	/*public static JavaPairRDD<String, Set<String>> runWithSets(JavaPairRDD<String, Set<String>> data1,
			JavaPairRDD<String, Set<String>> data2) {
		// TODO Auto-generated method stub
		data1 = PersonFilter.runSet(data1);
		data2 = PersonFilter.runSet(data2);
	        
		//data1 = AddDatasetIdFunc.run(data1, 1);
		//data2 = AddDatasetIdFunc.run(data2, 2);
	      
        data1 = data1.union(data2);
		return null;
	}


	public static void setResourceType(String string) {
		// TODO Auto-generated method stub
		
	}


	public static void setPredicatesToKeep(List<String> predicates) {
		// TODO Auto-generated method stub
		
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

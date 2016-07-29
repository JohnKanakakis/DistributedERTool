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
import spark.io.DataReader;


public class ResourceFilter {
	public static Logger logger = LoggerFactory.getLogger(ResourceFilter.class);
	public final static Property TYPE_PROPERTY = RDF.type;
	
    private static boolean hasCorrectProperties(Set<Tuple2<String, String>> resourceProperties, HashSet<String> configProperties){
    	
    	HashSet<String> resourcePropertiesNames = new HashSet<String>();
    	for(Tuple2<String, String> po : resourceProperties)
    		resourcePropertiesNames.add(po._1);
    	
    	if(resourcePropertiesNames.containsAll(configProperties))
    		return true;
    	
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
}



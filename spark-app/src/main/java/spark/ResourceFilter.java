package spark;



import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.io.config.KBInfo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.vocabulary.RDF;

import scala.Tuple2;


/**
 * The ResourceFilter filters the data according to the LIMES configuration
 * @author John Kanakakis
 *
 */
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
    
   
	/**
	 * @param records : the input data RDD
	 * @param kbB : 
	 * the broadcasted KBInfo object which holds the information of
	 * the entities (type class, properties etc) involved in the linking task 
	 * @return the filtered data in the form of (r_id, [info]) 
	 */
	public static JavaPairRDD<String, List<String>> run(
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



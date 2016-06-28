package spark.differential;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class DifferenceFunction<S,L> implements FlatMapFunction<Iterator<String>, String> {

	/**
	 * 
	 */
	final static Logger logger = Logger.getLogger(DifferenceFunction.class);
	private static final long serialVersionUID = 1L;
	Broadcast<List<String>> links;
	
	@Override
	public Iterable<String> call(Iterator<String> sourcePartition) throws Exception {
		
		Iterator<String> l = links.getValue().iterator();
		
		Set<String> ls = new HashSet<String>();
		//HashMap<String,String> lmap = new HashMap<String,String>(); 
		
		String quad = null;
		String s = null;
		int sample = 5;
		while(l.hasNext()){
			quad = l.next();
			
			s = quad.split(" ")[0];
			if(sample > 0){
				logger.info("quad is "+quad);
				logger.info("s is "+s);
				sample--;
			}
			ls.add(s);
			//lmap.put(s,quad);
		}
		
		Set<String> ss = new HashSet<String>();
		HashMap<String,String> smap = new HashMap<String,String>(); 
		
		while(sourcePartition.hasNext()){
			quad = sourcePartition.next();
			s = quad.split(" ")[0];
			ss.add(s);
			smap.put(s, quad);
		}
		
		ss.removeAll(ls);
		
		
		List<String> result = new ArrayList<String>();
		Iterator<String> it = ss.iterator();
		sample = 5;
		String key = null;
		String value = null;
		while(it.hasNext()){
			key = it.next();
			value = smap.get(key);
			if(sample > 0){
				logger.info("value is "+value);
				sample--;
			}
			if(value != null){
				result.add(value);
			}
			
		}
		logger.info("RESULT SIZE = "+result.size() + " sample = "+result.get(3));
		return result;
	}
	
	public void setLinks(Broadcast<List<String>> linksB){
		this.links = linksB;
	}

}

package spark.filter;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PersonFilter {

	final static String FOAF_PERSON = "foaf:Person";//"{"+"\"a\""+":"+"\"foaf:Person\""+"}";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf sparkConf = new SparkConf().setAppName("DataFilter");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    ctx.close();
	}

	

	public static JavaPairRDD<String, Set<String>> runSet(JavaPairRDD<String, Set<String>> data) {
		// TODO Auto-generated method stub
		data = data.filter(new Function<Tuple2<String,Set<String>>,Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String,Set<String>> record) throws Exception {
				// TODO Auto-generated method stub
				String p1 = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
				String o1 = "http://xmlns.com/foaf/0.1/Person";
				
				String p2 = "http://xmlns.com/foaf/0.1/firstName";
				String p3 = "http://xmlns.com/foaf/0.1/lastName";
				
				Set<String> pos = record._2;
				
				boolean accepted = pos.contains(p1) && 
								   pos.contains(o1) &&
								   pos.contains(p2) &&
								   pos.contains(p3) ;
						  
				
				return accepted;
				
			}
		});
		return data;
	}

}


	/*data = data.filter(new Function<String,Boolean>(){

		*//**
		 * 
		 *//*
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(String triple) throws Exception {
			// TODO Auto-generated method stub
			String predicate = "a";
			
			Pattern predicatePtrn = Pattern.compile(predicate+" (.*);\\n"+"|"+predicate+" (.*)");
			Matcher matcher = predicatePtrn.matcher(triple);
			
			String value = null;
			while(matcher.find()){
				value = matcher.group(1);
				if(value == null){
					value = matcher.group(2);
				}
			}
			if(value == null){
				return false;
			}
			if(value.equals(FOAF_PERSON)){
				return true;
			}
			return false;
			
		}
		
	});*/


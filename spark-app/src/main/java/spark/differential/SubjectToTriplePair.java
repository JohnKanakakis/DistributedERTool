package spark.differential;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SubjectToTriplePair implements PairFunction<String, String, String> {

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, String> call(String triple) throws Exception {
		
		if (triple == null) return null;
		return new Tuple2<String,String>(triple.split(" ")[0], triple);
		
		
	}

}

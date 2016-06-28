package spark.creation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.Accumulable;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class CreateDataFunction<S,L> implements FlatMapFunction<Integer,String>, Serializable{

	
	private static final long serialVersionUID = 1L;
	private int num;
	
	String LEFT_ARROW = "<";
	String RIGHT_ARROW = ">";
	String SUBJECT = "http://example.com/John_";
	String PREDICATE1 = "http://example.com/works";
	String OBJECT1 = "http://example.com/ntua_";
	String PREDICATE2 = "http://example.com/lives_in";
	String OBJECT2 = "http://example.com/Nikea";
	String PREDICATE3 = "http://example.com/is";
	String OBJECT3 = "http://example.com/24";
	String SPACE = " ";
	String DATE_OF_COLLECTION = "oav:dateOfCollection: ";
	
	@Override
	public Iterable<String> call(Integer i) throws Exception {
		
		ArrayList<String> t = new ArrayList<String>();
		for(int k = i*this.num; k < (i+1)*this.num; k++){
			
			t.add(LEFT_ARROW + SUBJECT + k + RIGHT_ARROW + SPACE + 
				  LEFT_ARROW + PREDICATE1 +   RIGHT_ARROW + SPACE + 
				  LEFT_ARROW + OBJECT1 + k +  RIGHT_ARROW + SPACE );
			t.add(LEFT_ARROW + SUBJECT + k + RIGHT_ARROW + SPACE + 
					  LEFT_ARROW + PREDICATE2 +   RIGHT_ARROW + SPACE + 
					  LEFT_ARROW + OBJECT2 + k%((i+1)*this.num/2) +  RIGHT_ARROW + SPACE );
			t.add(LEFT_ARROW + SUBJECT + k + RIGHT_ARROW + SPACE + 
					  LEFT_ARROW + PREDICATE3 +   RIGHT_ARROW + SPACE + 
					  LEFT_ARROW + OBJECT3 + k%((i+1)*this.num/3) +  RIGHT_ARROW + SPACE );
					   //DATE_OF_COLLECTION+System.currentTimeMillis());
		}
		
		return t;
		// TODO Auto-generated method stub
				/*acclinks.add(SUBJECT + i+SPACE + 
						   PREDICATE + SPACE + 
						   OBJECT + +i+SPACE + 
						   DATE_OF_COLLECTION+System.currentTimeMillis());*/
	}

	public void setFractionOfTriples(int n) {
		// TODO Auto-generated method stub
		this.num = n;
	}
	
}

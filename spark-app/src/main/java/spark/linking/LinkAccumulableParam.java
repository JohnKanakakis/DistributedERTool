package spark.linking;

import java.util.ArrayList;

import org.apache.spark.AccumulableParam;

public class LinkAccumulableParam implements AccumulableParam<ArrayList<String>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	@Override
	public ArrayList<String> addAccumulator(ArrayList<String> links, String link) {
		// TODO Auto-generated method stub
		links.add(link);
		return links;
	}


	@Override
	public ArrayList<String> addInPlace(ArrayList<String> links1, ArrayList<String> links2) {
		// TODO Auto-generated method stub
		links1.addAll(links2);
		return links1;
	}


	@Override
	public ArrayList<String> zero(ArrayList<String> links) {
		// TODO Auto-generated method stub
		return new ArrayList<String>();
	}

}

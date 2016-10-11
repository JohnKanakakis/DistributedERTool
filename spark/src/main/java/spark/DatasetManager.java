package spark;

import java.io.Serializable;

/**
 * The DatasetManager is used to add a data set stamp to each resource URI
 * so as to distinguish between source and target URIs.
 * 
 * @author John Kanakakis
 *
 */
public class DatasetManager implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final String DATASET_STAMP = "_";
	private static DatasetManager dm = null;
	
	public static DatasetManager get() {
		// TODO Auto-generated method stub
		if(dm == null){
			dm = new DatasetManager();
		}
		return dm;
	}
	
	
	
	public static String addDatasetIdToResource(String resource, String datasetId){
		
		return resource+DATASET_STAMP+datasetId;
	}

	public static String getDatasetIdOfResource(String resource){
		
		int pos = resource.lastIndexOf(DATASET_STAMP)+DATASET_STAMP.length();
		String datasetId = resource.substring(pos);
		return datasetId;
	}

	public static String removeDatasetIdFromResource(String resource) {
		// TODO Auto-generated method stub
		int pos = resource.lastIndexOf(DATASET_STAMP);
		
		return resource.substring(0,pos);
	}
}

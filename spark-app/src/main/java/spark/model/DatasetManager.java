package spark.model;

import java.io.Serializable;
import java.util.HashMap;

public class DatasetManager implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static HashMap<Integer,DatasetInfo> datasets = new HashMap<Integer,DatasetInfo>();
	private static final String DATASET_STAMP = "_";
	private static DatasetManager dm = null;
	
	public static DatasetManager get() {
		// TODO Auto-generated method stub
		if(dm == null){
			dm = new DatasetManager();
		}
		return dm;
	}
	
	/*public void addDatasetInfo(DatasetInfo datasetInfo){
		datasets.put(datasetInfo.getId(), datasetInfo);
	}
	*/
	public DatasetInfo getDatasetInfo(int id){
		return datasets.get(id);
	}
	
	public DatasetInfo getDatasetOfResource(String resource){
		
		int pos = resource.lastIndexOf(DATASET_STAMP);
		int datasetId = Integer.parseInt(resource.substring(pos));
		return datasets.get(datasetId);
	}
	
	public static String addDatasetIdToResource(String resource, String datasetId){
		
		return resource+DATASET_STAMP+datasetId;
	}

	public static String getDatasetIdOfResource(String resource){
		
		int pos = resource.lastIndexOf(DATASET_STAMP)+DATASET_STAMP.length();
		String datasetId = resource.substring(pos);
		return datasetId;
	}
}

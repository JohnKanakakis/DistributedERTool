package spark.model;


import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

import org.aksw.limes.core.io.config.KBInfo;

import spark.Utils;

public class DatasetInfo implements Serializable{

	private static final long serialVersionUID = 1L;
	private String id;
	private String typeClass;
	private HashSet<String> properties; 
	
	
	/*public DatasetInfo(int id,String typeClass,List<String> p ){
		this.id = id;
		this.typeClass = typeClass;
		this.properties = new HashSet<String>();
		for(int i = 0; i < p.size(); i++){
			this.properties.add(p.get(i));
		}
	}*/
	
	public DatasetInfo(KBInfo kbInfo) {
		// TODO Auto-generated constructor stub
		this.properties = Utils.toHashSet(kbInfo.getProperties());
		this.typeClass = kbInfo.getClassRestriction();
		this.id = kbInfo.getId();
	}

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public HashSet<String> getProperties() {
		return properties;
	}
	public void setProperties(HashSet<String> properties) {
		this.properties = properties;
	}

	public String getTypeClass() {
		return typeClass;
	}

	public void setTypeClass(String typeClass) {
		this.typeClass = typeClass;
	}
	
	
}

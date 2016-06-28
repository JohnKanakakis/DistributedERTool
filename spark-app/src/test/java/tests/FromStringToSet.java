package tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;

import it.unimi.dsi.fastutil.BigList;

public class FromStringToSet {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*String s = "";
		try {
			s = FileUtils.readFileToString(new File("arraylist-test"));
			test1(s);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		test0();
	}

	public static void test1(String s){
		int pos = s.indexOf("[");
		s = s.substring(pos,s.length()-1);
		List<String> ar = Arrays.asList(s.split(","));
		
		for(int i = 0; i < ar.size();i++)
			System.out.println(ar.get(i));
	}
	
	public static void test0(){
			String s = "[http://www.w3.org/1999/02/22-rdf-syntax-ns#type@@@,   http://xmlns.com/foaf/0.1/Person@@@,  http://xmlns.com/foaf/0.1/firstName@@@, \"Gi, la\"^^<http://www.w3.org/2001/XMLSchema-datatypesstring>@@@,  http://xmlns.com/foaf/0.1/lastName@@@,\"Lithwick-yanai\"^^<http://www.w3.org/2001/XMLSchema-datatypesstring>@@@, http://lod.openaire.eu/vocab#isAuthorOf@@@, http://lod.openaire.eu/data/resultod_908_5aa4311a020b6a57d79e23635626a9@@@]";
			s = s.substring(1,s.length()-1);
			
			
			ArrayList<String> pos = new ArrayList<String>(Arrays.asList(s.split("@@@,|@@@")));
			
			String p = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
			String o = "http://xmlns.com/foaf/0.1/Person";
			
			/*System.out.println("|"+pos.get(1));
			
			int index = pos.indexOf(p);
			System.out.println("found at "+(index));
			if(index == pos.size()-1){
				System.out.println("malformed pos list"+pos);
				
			}
			if(pos.get(index+1).equals(o)){
				System.out.println("found at "+(index+1));
			}
			
			pos.add(0, "midfd");*/

			System.out.println("list="+s.split("@@@,|@@@"));
			for(int i = 0; i < pos.size();i++)
				System.out.println(pos.get(i));
	}
}

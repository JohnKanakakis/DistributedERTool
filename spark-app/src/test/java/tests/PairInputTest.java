package tests;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

public class PairInputTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			String s = FileUtils.readFileToString(new File("parenthesis-pair"));
			
			String[] predicates = new String[2];
			
			predicates[0] = "http://xmlns.com/foaf/0.1/firstName";//"foaf:firstName";
			predicates[1] = "http://xmlns.com/foaf/0.1/lastName";
			String key = null;
			for(int i = 0; i < predicates.length;i++){
				key = getValue(s,predicates[i]);
				System.out.println("key|"+key);
			}
		/*	int pos = s.indexOf(",");
			String key = s.substring(1, pos);
			String value = s.substring(pos+1);
			System.out.println("key|"+key);
			System.out.println("value|"+value);*/
			/*Pattern pairPtrn = Pattern.compile("\\((.+)//,{1}+(.+)\\)",Pattern.DOTALL);
			
			
			Matcher matcher = pairPtrn.matcher(s);
			
			String key = null;
			String value = null;
			while(matcher.find()){
				key = matcher.group(1);
				value = matcher.group(2);
				System.out.println("key|"+key);
				System.out.println("value|"+value);
				
			};*/
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public static String getValue(String s,String predicate){
		
		/*Pattern predicatePtrn = Pattern.compile("\\("+predicate+"\\,"+"(.*)"+"\\){1}+");
		
		Matcher matcher = predicatePtrn.matcher(s);
		
		String value = null;
		matcher.find();
		value = matcher.group(0);*/
			
		int pos = s.indexOf(predicate);
		s = s.substring(pos+predicate.length()+1);
		int pos1 = s.indexOf("^");
		String value = s.substring(0, pos1);
		return value;
	}
}

package tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

public class InputTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		String s = "<http://swrc.ontoware.org/ontology#month> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .";
		
		int pos1;
		int pos2;
		String p;
		int cnt = 0;
		ArrayList<String> triple = new ArrayList<String>();
		boolean validTriple = true;
		while(validTriple){
			pos1 = s.indexOf("<");
			pos2 = s.indexOf(">");
			if(pos1 == -1 || pos2 == -1 || pos2 < pos1){
				if(cnt == 3){
					System.out.println("valid triple..exiting");
					break;
				}else{
					System.out.println("invalid triple..exiting");
					validTriple = false;
				}
			}else{
				p = s.substring(pos1+1,pos2);
				triple.add(p);
				s = s.substring(pos2+1);
				//System.out.println(s);
				cnt++;
			}
		
		}
		if(validTriple)
			System.out.println(triple);
		//test13();
	    /*try {
			test2();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	private static void test3(){
		String s = "(<http://example.com/John_66344052>,"
				+       "<http://example.com/works>###<http://example.com/ntua_66344052>"
				+ "???"
				+ "<http://example.com/lives_in>###<http://example.com/Nikea33144052>"
				+ "???"
				+ "<http://example.com/is>###<http://example.com/2422077386>)";
		s = s.substring(1,s.length()-1);
		System.out.println(s);
		String[] s1 = s.split(",");
		
		System.out.println(s1[0]+" "+s1[1]);
		String regex = "\\?{3}";
		Pattern p = Pattern.compile(regex);
		String[] ps = s1[1].split(regex);
	  
		for(int i = 0; i < ps.length; i++){
		   String t[] = ps[i].split("\\#{3}");
		   System.out.println(t[0]+" "+t[1]);
		}
	}
	private static void test2() throws IOException {
		
		String l = FileUtils.readFileToString(new File("input.ttl"));
		System.out.println(l);
		System.out.println("###############################");
		// TODO Auto-generated method stub
		String[] lines = l.split("\\.\n++");
		String[] POs = null;
		String subject = null;
		String firstPO = null;
		String subjectMap = null;
		String[] splitPO = null;
		for(int i = 0; i < lines.length;i++){
			if(lines[i].startsWith("@")) continue;
			
			POs = lines[i].split("\\;\\n\\t++");
			
			subject = POs[0].split("\\n\\t++")[0];
			firstPO = POs[0].split("\\n\\t++")[1];
			
			splitPO = firstPO.replace("\"", "'").split(" ",2);
			
			
			subjectMap = "{\"values\":"+"["+"{\""+splitPO[0]+"\":\""+splitPO[1]+"\"}";
			for(int j = 1; j < POs.length;j++){
				
				splitPO = POs[j].replace("\"", "'").split(" ",2);
				subjectMap+=(","+"{\""+splitPO[0]+"\":\""+splitPO[1]+"\"}");
			}
			subjectMap+=("]}");
			System.out.println(subjectMap);
			System.out.println("###############################");
		}
		
	}

	private static void test4(){
		String s = "oav:openairecompatibility" +"\t\t\t\"not available\""+"^^xsd:string;";
		System.out.println(s);
		
		String t[] = s.split(" ++|\t++", 2);
		System.out.println(t[0]);
		System.out.println(t[1]);
	}
	private static void test1() {
		// TODO Auto-generated method stub
		String s = "@prefix oad: <http://lod.openaire.eu/data/> .";
		String s1 = "oad:datasourcedoajarticles_0008c1d3d44419a8a487949b14dd640c";
		System.out.println(s1.startsWith("@prefix"));
		
		String s2 = "dddfdf.dfdf.\n"
				+ "fdfdf.dfd;fd";
		System.out.println(s2);
		System.out.println("first line = "+s2.split("\\;")[0]);
	}

	
	private static void test5(){
		
		String s;
		try {
			s = FileUtils.readFileToString(new File("input1.ttl"));
			
			System.out.println("s = "+ s);
			String foafFirstname = "foaf:firstName";
			String foafLastname = "foaf:lastName";
			
			Pattern firstNamePtrn = Pattern.compile("\\{\""+foafFirstname+"\":\"(.*?)\"\\}");
			Pattern lastNamePtrn = Pattern.compile("\\{\""+foafLastname+"\":\"(.*?)\"\\}");
			
			Matcher matcher = firstNamePtrn.matcher(s);
			matcher.find();
			String firstname = matcher.group(1);
			matcher = lastNamePtrn.matcher(s);
			matcher.find();
			String lastname = matcher.group(1);
			
			String key1 = firstname.substring(0, firstname.indexOf("^^xsd:string")).replace("'", "");
			String key2 = lastname.substring(0, lastname.indexOf("^^xsd:string")).replace("'", "");
		
			System.out.println(key1);
			System.out.println(key2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void test6(){
		String s = "(Moneruzzaman, K. M.,oad:persondoajarticles_099c950bf3a79eb6a84dbec15af2d8f)";
		
		Pattern namePtrn = Pattern.compile("((.*?),)");
		
		String key = s.substring(1,s.lastIndexOf(","));
		System.out.println(key);
		String value = s.substring(s.lastIndexOf(",")+1,s.length()-1);
		System.out.println(value);
		/*Matcher matcher = namePtrn.matcher(s);
		while(matcher.find()){
			String value = matcher.group(1);
			System.out.println(value);
		}*/
		
	}
	
	public static void test7(){
		 
		String base = "fdfd/fdfd/";
		List<String> filesInDirectory = new ArrayList<String>();
        for(int i = 0; i < 197; i++){
        	if(i < 10)
        		filesInDirectory.add(base+"/part-m-0000"+i+".ttl");
        	else if(i < 100)
        		filesInDirectory.add(base+"/part-m-000"+i+".ttl");
        	else
        		filesInDirectory.add(base+"/part-m-00"+i+".ttl");
        }
        for(int i = 0; i < filesInDirectory.size(); i++){
        	System.out.println(filesInDirectory.get(i));
        }
	}
	
	/*predicate matching*/
	public static void test10(){
		
		try {
			String s = FileUtils.readFileToString(new File("input2.ttl"));
			String predicates[] = new String[4];
			predicates[0] = "dcterms:language";
			predicates[1] = "##foaf:homePage";
			predicates[2] = "oav:fdfd";
			predicates[3] = "a";
			
			for(int i = 0; i < predicates.length;i++){
				String value = test11(s,predicates[i]);
				if(value != null)
					System.out.println("value|"+value);
				else
					System.out.println("no value");
			};
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static String test11(String s, String predicate){
		
		
		Pattern predicatePtrn = Pattern.compile(predicate+" (.*);\\n"+"|"+predicate+" (.*)");
		
		Matcher matcher = predicatePtrn.matcher(s);
		
		String value = null;
		if(matcher.find()){
			value = matcher.group(1);
			if(value == null){
				value = matcher.group(2);
			}
		};
		//while(matcher.find()){
			
			
			//System.out.println("value|"+value);
		//}
		return value;
	}

	public static void test12(){
		
		String s = "";
		try {
			s = FileUtils.readFileToString(new File("input2.ttl"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String triple = s;
		int pos = triple.indexOf("\n");
		String subject = triple.substring(0, pos);
		/*Pattern subjectPtrn = Pattern.compile("(.*)");
		Matcher matcher = subjectPtrn.matcher(triple);
		matcher.find();
		
		String subject = matcher.group(1);*/
		String new_subject = subject+"_copy";
		triple = triple.replace(subject, new_subject);
		System.out.println(triple);
	}
	
	public static void test13(){
		
		String s = "";
		try {
			s = FileUtils.readFileToString(new File("input2.ttl"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Pattern emptyPtrn = Pattern.compile("(\\n*)(.*)",Pattern.DOTALL);
		Matcher matcher = emptyPtrn.matcher(s);
		matcher.find();
		
		String triple = matcher.group(2);
		
		System.out.println(triple);
	}
}

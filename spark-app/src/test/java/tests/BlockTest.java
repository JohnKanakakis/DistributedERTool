package tests;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;


public class BlockTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	
		String s = "";
		try {
			s = FileUtils.readFileToString(new File("input.ttl"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//readTTL(s);
		/*
		readBlock(s);*/
		/*Pattern subjectPtrn = Pattern.compile("(.*)");
		Matcher matcher = subjectPtrn.matcher(s);
		matcher.find();
		
		String subject = matcher.group(1);
		String new_subject = subject+"_copy";
		s = s.replace(subject, new_subject);
		System.out.println(s);*/
	}

	
	public static void readBlock(String record){
		
		record = record.substring(1);
		Pattern keyPtrn = Pattern.compile("((.*)\\^\\^xsd:string,)");
		//Pattern valuePrtn = Pattern.compile("(.*)@@",Pattern.DOTALL);
		Matcher matcher = keyPtrn.matcher(record);
		matcher.find();
		
		String key = matcher.group(1);
		
		
		System.out.println(key);
		record = record.replaceFirst(keyPtrn.toString(), "");
		System.out.println("new record|"+record);
		String[] values = record.split("@@");
		for(int i = 0; i < values.length; i++){
			System.out.println("value!"+values[i]);
			analyzeTriples(values[i]);
		}
		/*matcher = valuePrtn.matcher(record);
		while(matcher.find()){
			System.out.println("value!"+matcher.group(1));
		}*/
	}


	private static void analyzeTriples(String triple) {
		// TODO Auto-generated method stub
		int pos_n = triple.indexOf("\n");
		String subject = triple.substring(0,pos_n);
		int pos_ = subject.lastIndexOf("_");
		String subject_id = subject.substring(0, pos_);
		System.out.println("subject id is "+subject_id);
		
		String dataset_id = subject.substring(pos_+1);
		System.out.println("dataset id is "+dataset_id);
		
		

		
	}
	
	/*private static void readTTL(String s ){
		
		try {
			//in = new FileInputStream("input.ttl");
			RDFParser p = Rio.createParser(RDFFormat.TURTLE);
			
			ArrayList<Statement> myList = new ArrayList<Statement>();
			StatementCollector collector = new StatementCollector(myList);
			p.setRDFHandler(collector);
			
			
			InputStream in = new ByteArrayInputStream(s.getBytes());
			
			p.parse(in,"");
			
			for(int i = 0 ; i < myList.size(); i++){
				System.out.println(myList.get(i).toString());
			}
		} catch (IOException | RDFParseException | RDFHandlerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}*/
}

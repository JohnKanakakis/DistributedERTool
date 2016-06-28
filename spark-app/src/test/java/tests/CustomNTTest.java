package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;

import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import scala.Tuple2;
import spark.filter.DataFormatter;
import spark.io.CustomNTriplesParser;

public class CustomNTTest {

	public static void main(String[] args) throws RDFParseException, RDFHandlerException, IOException {
		// TODO Auto-generated method stub

		
		List<Integer> l = new ArrayList<Integer>();
		for(int i = 0; i < 5; i++){
			l.add(i);
		}
		Iterator<Integer> it = l.iterator();
		while(it.hasNext()){
			System.out.println(it.next());
		}
		 it = l.iterator();
		while(it.hasNext()){
			System.out.println(it.next());
		}
		/*
		ObjectBigArrayBigList<String> result = 
				new ObjectBigArrayBigList<String>();
		
		ObjectBigArrayBigList<String> buffer = 
				new ObjectBigArrayBigList<String>(1000000);
		
		for(int i = 0; i < 5; i++){
			for(int j = 0; j < 1000000; j++){
				buffer.add("e"+j);
			}
			System.out.println("buffer size before = "+buffer.size64());
			
			result.addAll(buffer);
			System.out.println("result size = "+result.size64());
			//buffer.removeElements(0, buffer.size64());
			buffer.clear();
			System.out.println("buffer size after = "+buffer.size64());
			System.out.println("########################");
		}*/
		
		/*String triple;
		List<String> t = new ArrayList<String>(3);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("input.nt"))));
		while( (triple = br.readLine()) != null){
			t = DataFormatter.toTriple(triple,t);
			if(t.get(0) == "" && t.get(1) == "" && t.get(2) == ""){
				continue;
			}
			System.out.println(t);
			t.clear();
		}
		br.close();*/
		/*CustomNTriplesParser parser = new CustomNTriplesParser();
		InputStream in = new FileInputStream(new File("input.nt"));
		parser.parse(in, "");
		in.close();
		System.out.println(parser.getResult());*/
	}

}

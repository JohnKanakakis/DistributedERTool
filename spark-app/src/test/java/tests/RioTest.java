package tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;


import scala.Tuple2;

public class RioTest {

	/*public static void main(String[] args) throws RDFParseException, RDFHandlerException, IOException {
		// TODO Auto-generated method stub

		InputStream in = new FileInputStream(new File("input.nt"));
		RDFParser parser = new NTriplesParserFactory().getParser();//Rio.createParser(RDFFormat.NTRIPLES);
		
		ArrayList<Statement> statements = new ArrayList<Statement>();
		StatementCollector collector = new StatementCollector(statements);
		parser.setRDFHandler(collector);

		parser.parse(in,"");
		Statement st = null;
		Tuple2<String,String> po = null;
		String s = null;
		String p = null;
		String o = null;
		for(int i = 0 ; i < statements.size(); i++){
			st = statements.get(i);
			s = st.getSubject().toString();
			p = st.getPredicate().toString();
			o = st.getObject().toString();
			//po = new Tuple2<String,String>(p,o);
			System.out.println(st.toString());
			//result.add(new Tuple2<String,Tuple2<String,String>>(s,po));
		}

		
	}*/

}

package tests;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;
import org.scilab.forge.jlatexmath.Char;

import com.google.common.primitives.Chars;

public class TextTest {

	public static void main(String[] args) throws CharacterCodingException {
		// TODO Auto-generated method stub

		String[] ar = {"ffdf","ddf"};
		
		
		Text t1 = new Text("<http://lod.openaire.eu/data/person/doajarticles::de26c708146cf8e9047289ddd5fd5e48>	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>	<http://xmlns.com/foaf/0.1/Person> .");
		Text t2 = new Text("<http://lod.openaire.eu/data/person/od_______307::130519beae4c4ebd45797219f9ba721d>	<http://xmlns.com/foaf/0.1/firstName> \"Alexandra Sobral\" .");
		
		Text t = t2;
		byte[] b = t.getBytes();
		int pos1 = t.find("<");
		int pos2 = t.find(">");
		
		String s = Text.decode(b, pos1+1, pos2-pos1-1);
		System.out.println(s);
		pos1 = t.find("<",pos2+1);
		pos2 = t.find(">",pos1+1);
		
		String p = Text.decode(b, pos1+1, pos2-pos1-1);
		System.out.println(p);
		
		
		pos1 = pos2;
		while(t.charAt(pos1) != 9 && t.charAt(pos1) != ' ' && pos1 < t.getLength()){// 9 equals to tab
			//System.out.println("c:"+Chars.t.charAt(pos1));
			pos1++;
		}
		pos1++;
		//System.out.println("tab reached");
		//System.out.println("next char = "+t.charAt(pos1));
		
		
		if(t.charAt(pos1) == 34){// 34 equals to "\""
			//object is literal
			//pos1 = t.find("\"",pos1+1);
			
			String o = Text.decode(b, pos1,t.getLength()-pos1-2);
			System.out.println(o);
		}else if (t.charAt(pos1) == 60){// 60 equals to "<"
			//object is URI
			pos2 = t.find(">",pos1+1);
			String o = Text.decode(b, pos1+1,pos2 - pos1 -1);
			System.out.println(o);
		}else{
			System.out.println("wrong");
		}
		
		
	}

}

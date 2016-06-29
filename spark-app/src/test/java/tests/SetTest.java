package tests;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import com.hp.hpl.jena.vocabulary.OWL;

public class SetTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ArrayList<String> l = new ArrayList<String>();
		for(int i = 0; i < 10; i++)
			l = increase(l,i);
		System.out.println(l);
		
	}

	public static ArrayList<String> increase(ArrayList<String> l,int i){
		l.add("e"+i);
		return l;
	}
}

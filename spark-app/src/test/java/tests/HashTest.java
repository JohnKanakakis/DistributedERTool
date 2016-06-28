package tests;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.codec.language.Soundex;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashBigSet;
import scala.Tuple2;
import spark.BlocksCreator;

public class HashTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int W = 1;
		int L = 6;
		//int k = L/W;
		String[] names = {"Ilkka Pörsti","Ilias Flaounas","Ilkka Pörsti","fdfd","john","Ilias Flaounas"};
		List<Tuple2<String,String>> l = new ArrayList<Tuple2<String,String>>();
		
		int S = 1;
		for(int j = 0; j < S; j++){
			for(int i = 0; i < names.length;i++){
				l.add(new Tuple2<String,String>("e"+j+"-"+i,names[i]));
			}
		}
		
			
		
		//Collections.sort(l,new Tuple2Comparator());
		System.out.println(l.get(0));
		System.out.println(l.get(1));
		System.out.println(l);
		
		
		for(int i = 0; i < Math.max(L-W+1,1); i++){
			System.out.println(i+"-"+(Math.min(i+W,L) -1));
			for(int j = i; j < Math.min(i+W,L); j++){
				
			}
		}
		System.out.println(Math.max(L-W+1,1) + " blocks");
		System.exit(0);		
		
		/*ObjectOpenHashBigSet<String> set = new ObjectOpenHashBigSet<String>();
		
		for(int i = 0; i < 1000000000; i++){
			set.add("e"+i);
		}*/
		
		/*ObjectArrayList<Tuple2<String, String>> l = new ObjectArrayList<Tuple2<String,String>>();
		
		l.add(new Tuple2<String,String>("e","e"));
		l.add(new Tuple2<String,String>("b","b"));
		
		Comparator<Tuple2<String, String>> c = new Tuple2Comparator();
		
		@SuppressWarnings("unchecked")
		Tuple2<String, String>[] a = (Tuple2<String, String>[]) new Object[10];
		ObjectArrays.quickSort(l.toArray(a),c);
		System.out.println(a);
		
		String value = "dfdfd0d4s5dsdf$#%&$*#(df";
		value = value.replaceAll("\\p{Digit}|\\p{Punct}", "");
		System.out.println(value);
		Soundex soundex = new Soundex();
		System.out.println(soundex.encode(value));*/
		/*MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		messageDigest.update(value.getBytes());
		System.out.println(new String(messageDigest.digest()).toString());*/
	}

}

class Tuple2Comparator implements Comparator<Tuple2<String,String>>{

	public int compare(Tuple2<String, String> t1, Tuple2<String, String> t2) {
		// TODO Auto-generated method stub
		String p1 = t1._2;
		String p2 = t2._2;
		return p1.compareTo(p2);
	}

}
package spark;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashBigSet;
import scala.Tuple2;

public class Utils {

	public static void sample(JavaRDD<?> rdd , int num){
		
		List<?> sample = rdd.take(num);
		for(int i = 0; i < num; i++){
			System.out.println("record|"+sample.get(i).toString());
		}
	}

	public static void sample(JavaPairRDD<?,?> rdd, int num) {
		// TODO Auto-generated method stub
		List<?> sample = rdd.take(num);
		for(int i = 0; i < num; i++){
			System.out.println("pair|"+sample.get(i).toString());
		}
	}
	
	public static String getContentOfHDFSFile(String file){
		org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = null;
		String fileContent = "";
		InputStream in = null;
		try {
			hdfs = FileSystem.get(hdfsConf);
			//in = hdfs.open(new Path(file));
			fileContent = IOUtils.toString(in);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//return in;
		return fileContent;
	}
	
	public static InputStream getHDFSFile(String file){
		org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = null;
		//String fileContent = "";
		InputStream in = null;
		try {
			hdfs = FileSystem.get(hdfsConf);
			in = hdfs.open(new Path(file));
			//fileContent = IOUtils.toString(in);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return in;
		//return fileContent;
	}
	
	public static byte[] serialize(Object object) {
		// TODO Auto-generated method stub
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
		    ObjectOutput out = new ObjectOutputStream(bos);
	        out.writeObject(object);
	        out.close();
	        bos.close();
	        return bos.toByteArray();
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}

	public static Object deserialize(byte[] bytes)  {
	    try {
    		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
	        ObjectInput in = new ObjectInputStream(bis);
	        Object obj = in.readObject();
	        in.close();
	        bis.close();
	        return obj;
	    } catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	    return null;
	}

	public static <V> HashSet<V> toHashSet(List<V> list) {
		// TODO Auto-generated method stub
		HashSet<V> set = new HashSet<V>();
		for(int i = 0; i < list.size(); i++){
			set.add(list.get(i));
		}
		return set;
	}
	
	/*
	public static <V, U> Function2<U, V, U> getSeqFunc(Class<?> U, Class<?> V){
		
		return new Function2<U,V,U>(){
			private static final long serialVersionUID = 1L;

			@Override
			public U call(U arg0, V arg1) throws Exception {
				// TODO Auto-generated method stub
				((Set<?>) arg0).add(arg1);
				return null;
			}
		};
	}
	*/
	
	public static Function2<ObjectOpenHashBigSet<Tuple2<String, String>>, ObjectOpenHashBigSet<Tuple2<String, String>>, ObjectOpenHashBigSet<Tuple2<String, String>>> getCombFunc(){
	
		return
		  new Function2<ObjectOpenHashBigSet<Tuple2<String, String>>, 
		  ObjectOpenHashBigSet<Tuple2<String, String>>, 
		  ObjectOpenHashBigSet<Tuple2<String, String>>>(){

				private static final long serialVersionUID = 1L;
		
				@Override
				public ObjectOpenHashBigSet<Tuple2<String, String>> call(ObjectOpenHashBigSet<Tuple2<String, String>> set1,
						ObjectOpenHashBigSet<Tuple2<String, String>> set2) throws Exception {
					// TODO Auto-generated method stub
					set1.addAll(set2);
					return set1;
				}

		};
	}
	
	public static Function2<ObjectOpenHashBigSet<String>, String, ObjectOpenHashBigSet<String>> getSeqFunction(){
		return 
		new Function2<ObjectOpenHashBigSet<String>, 
	  		  		  String, 
	  		  		ObjectOpenHashBigSet<String>>(){

				private static final long serialVersionUID = 1L;

				@Override
				public ObjectOpenHashBigSet<String> call(ObjectOpenHashBigSet<String> set,
						  String t) throws Exception {
					// TODO Auto-generated method stub
					set.add(t);
					return set;
				}

		};	
	}

	public static Function2<Set<String>, Set<String>, Set<String>> getSeqFunction1() {
		// TODO Auto-generated method stub
		return new Function2<Set<String>, Set<String>, Set<String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Set<String> call(Set<String> set1,
					Set<String> set2) throws Exception {
				// TODO Auto-generated method stub
				set1.addAll(set2);
				return set1;
			}
		};
	}

	public static Function2<Set<Tuple2<String, String>>, Set<Tuple2<String, String>>, Set<Tuple2<String, String>>> getCombFunc1() {
		// TODO Auto-generated method stub
		return new Function2<Set<Tuple2<String, String>>, Set<Tuple2<String, String>>, Set<Tuple2<String, String>>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Set<Tuple2<String, String>> call(Set<Tuple2<String, String>> set1, Set<Tuple2<String, String>> set2)
					throws Exception {
				// TODO Auto-generated method stub
				set1.addAll(set2);
				return set1;
			}
			
		};
	}

	public static <K, V> Set<Tuple2<K, V>> toSet(Map<K,V> map) {
		// TODO Auto-generated method stub
		Set<Tuple2<K,V>> result = new HashSet<Tuple2<K,V>>();
		Iterator<Entry<K, V>> it = map.entrySet().iterator();
		Entry<K, V> entry; 
		while(it.hasNext()){
			entry = it.next();
			result.add(new Tuple2<K,V>(entry.getKey(),entry.getValue()));
		}
		return result;
	}

	public static <V, K> JavaPairRDD<K, Integer> countPairs(JavaPairRDD<K, V> pairs) {
		
		return pairs.aggregateByKey(0, new Function2<Integer,V,Integer>(){

		private static final long serialVersionUID = 1L;

		@Override
		public Integer call(Integer arg0, V arg1) throws Exception {
			// TODO Auto-generated method stub
			return arg0+1;
		}
		}, new Function2<Integer,Integer,Integer>(){

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
	
		@Override
		public Integer call(Integer arg0, Integer arg1) throws Exception {
			// TODO Auto-generated method stub
			return arg0+arg1;
		}
		
	});
	}

	public static void deleteHDFSFile(String file) {
		// TODO Auto-generated method stub
		org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = null;
		
		try {
			hdfs = FileSystem.get(hdfsConf);
			hdfs.delete(new Path(file), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

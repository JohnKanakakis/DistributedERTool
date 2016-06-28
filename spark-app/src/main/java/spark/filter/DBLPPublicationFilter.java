package spark.filter;

import java.util.ArrayList;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import scala.Tuple2;

import spark.ResourceFilter;

public class DBLPPublicationFilter {
	public static Logger logger = LoggerFactory.getLogger(DBLPPublicationFilter.class);
	public static String[] words = {"letter from the editor","letter to the editor","editor","letter","preface"};
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		SparkConf sparkConf = new SparkConf().setAppName("DBLPPublicationFilter");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
    	JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> data = ctx.objectFile(args[0]);
    	
    	final ArrayList<String> classes = new ArrayList<String>();
    	
    	for(int i = 2; i < args.length;i++){
    		classes.add(args[i]);
    	}
    	
    	JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> articles = 
    	data
    	.filter(new Function<Tuple2<String,Set<Tuple2<String,String>>>,Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, String>>> record) throws Exception {
				// TODO Auto-generated method stub
			
				for(Tuple2<String,String> po : record._2){
					if(po._1.equals(ResourceFilter.TYPE_PROPERTY.toString())){
						for(int i = 0; i < classes.size();i++){
							if(po._2.equals(classes.get(i))){
								return true;
							}
						}
					}
				}
				return false;
			}
    	})
    	.distinct()
    	.cache();
    	
    	JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> special = 
    	articles.filter(new Function<Tuple2<String,Set<Tuple2<String,String>>>,Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, String>>> record) throws Exception {
				// TODO Auto-generated method stub
				
				for(Tuple2<String,String> po : record._2){
					if(po._1.equals("http://www.w3.org/2000/01/rdf-schema#label") && 
					   !hasNormalTitle(po._2)){
						return true;
					}
				}
				return false;
			}

			private boolean hasNormalTitle(String title) {
				// TODO Auto-generated method stub
				
				for(int i = 0; i < words.length;i++){
					if(title.toLowerCase().contains(words[i]))
						return false;
				}
				return true;
			}
    	}).cache();
    	//special.saveAsTextFile(args[1]);
    	
    	ArrayList<String> result = new ArrayList<String>();
    	result.add("total articles are "+articles.count());
    	result.add("special articles are "+special.count());
    	
    	ctx.parallelize(result).coalesce(1).saveAsTextFile("/user/kanakakis/dblp_stats");
    	

    	
    	ctx.close();
	}

}

package spark.version1;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.BlocksCreator;
import spark.IndexCreator;
import spark.Linker;
import spark.ResourceFilter;
import spark.Utils;
import spark.model.DatasetInfo;
import spark.statistics.BlockStatistics;

public class ControllerNew {

	private static final String STATS_FILE = "/user/kanakakis/stats_file";
	public static Logger logger = LoggerFactory.getLogger(ControllerNew.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Utils.deleteHDFSFile(STATS_FILE);
		Utils.deleteHDFSFile(args[6]+"/oneToOneLinks");
	    Utils.deleteHDFSFile(args[6]+"/oneToNLinks");
	    
    	InputStream configFile = Utils.getHDFSFile(args[0]);
    	InputStream dtdFile = Utils.getHDFSFile(args[1]);
    	
    	XMLConfigurationReader reader = new XMLConfigurationReader();
		org.aksw.limes.core.io.config.Configuration config = null;
		config = reader.validateAndRead(configFile,dtdFile);
    	
		if(config == null){
			System.exit(0);
			return;
		}
		
		/*DatasetInfo d1 = new DatasetInfo(config.getSourceInfo());
		DatasetInfo d2 = new DatasetInfo(config.getTargetInfo());*/
		
		
		
		Rewriter rw = RewriterFactory.getRewriter("Default");
        LinkSpecification ls = new LinkSpecification(config.getMetricExpression(), config.getVerificationThreshold());
        LinkSpecification rwLs = rw.rewrite(ls);
	    IPlanner planner = ExecutionPlannerFactory.getPlanner(config.getExecutionPlan(), null, null);
        assert planner != null;
        NestedPlan plan = planner.plan(rwLs);

        byte[] planBinary = Utils.serialize(plan);
        byte[] skbBinary = Utils.serialize(config.getSourceInfo());
        byte[] tkbBinary = Utils.serialize(config.getTargetInfo());
        
        String[] configParams = new String[2];
        configParams[0] = config.getSourceInfo().getVar();
        configParams[1] = config.getTargetInfo().getVar();
        
        double thres = config.getAcceptanceThreshold();
        
        
       
        
    	SparkConf sparkConf = new SparkConf().setAppName("ControllerNew");
    
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
    	
		Broadcast<byte[]> b = ctx.broadcast(planBinary);
		Broadcast<String[]> c = ctx.broadcast(configParams);
		Broadcast<Double> t = ctx.broadcast(thres);
		Broadcast<byte[]> skb = ctx.broadcast(skbBinary);
		Broadcast<byte[]> tkb = ctx.broadcast(tkbBinary);
		
		
		JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> records1 = ctx.objectFile(config.getSourceInfo().getEndpoint());
																						 
		JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> records2 = ctx.objectFile(config.getTargetInfo().getEndpoint());
        
		
		JavaPairRDD<String, List<String>> resources1 = ResourceFilter.runWithPairs(records1,skb);//.coalesce(1000,false);
		JavaPairRDD<String, List<String>> resources2 = ResourceFilter.runWithPairs(records2,tkb);//.coalesce(1000,false);;
		
        JavaPairRDD<String, List<String>> resources = resources1.union(resources2)
        														.setName("resources");
        														//.partitionBy(new HashPartitioner(2000));
        
        
        //resources.saveAsObjectFile(args[4]);
		/*JavaRDD<Tuple2<String, List<String>>> data = ctx.objectFile("/user/kanakakis/publications");
		JavaPairRDD<String, List<String>> resources = 
		data.mapToPair(new PairFunction<Tuple2<String, List<String>>,String,List<String>>(){
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, List<String>> call(Tuple2<String, List<String>> t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String,List<String>>(t._1,t._2);
					}
					 
		});*/
		JavaPairRDD<String, List<String>> blocks = 
	        		IndexCreatorNew.createBlocks(resources,skb,tkb)
	        					   .setName("blocks")
	        					   .persist(StorageLevel.MEMORY_ONLY_SER());											 
		
		
		int optimalSize = 20000;
	    blocks.saveAsTextFile(args[5]);
		//JavaPairRDD<String, Set<Tuple2<String, Double>>> links = Linker.runWithList(blocks, skb, tkb, b, c, t,optimalSize);
	    //links.saveAsTextFile(args[6]);
	     /*
       
        					   //.persist(StorageLevel.MEMORY_ONLY_SER());
        
        long blocksCnt = blocks.count();
        JavaPairRDD<String, Set<List<String>>> blocks2 = 
        		IndexCreatorNew.createBlocks(resources2,skb,tkb)
        					   .setName("blocks2");
        					  // .persist(StorageLevel.MEMORY_AND_DISK_SER());
        
        
        JavaPairRDD<String, Set<List<String>>> blocks = 
        		blocks1.join(blocks2)
					   .mapValues(new Function<Tuple2<Set<List<String>>,Set<List<String>>>,Set<List<String>>>(){
						private static final long serialVersionUID = 1L;
	
						@Override
						public Set<List<String>> call(
								Tuple2<Set<List<String>>, Set<List<String>>> v1)
								throws Exception {
							// TODO Auto-generated method stub
							v1._1.addAll(v1._2);
							return v1._1;
						}
					   }).persist(StorageLevel.DISK_ONLY());
        
        
        JavaPairRDD<Integer, Integer> blocksFreq = IndexCreatorNew.getFrequencyOfBlocks(blocks);
        													   //.persist(StorageLevel.MEMORY_AND_DISK_SER());
        List<Tuple2<Integer, Integer>> blockSizes = blocksFreq.collect();
        
        int optimalSize = BlockStatistics.getOptimalBlockSize(blockSizes);
       
        
        ArrayList<String> result = new ArrayList<String>();
        result.add("optimal size:"+optimalSize);
        result.add("blocks after filter "+blocksCnt);
        //result.add("links = "+links.count());
        //result.add("oneToNLinks "+oneToNLinks.count());
       
        
        ctx.parallelize(result,1).saveAsTextFile(STATS_FILE);*/
	
        //blocks.unpersist();
        //links.persist(StorageLevel.MEMORY_AND_DISK_SER()).setName("links");
        
        
        /*JavaPairRDD<String, Set<Tuple2<String, Double>>> oneToNLinks = 
        links.filter(new Function<Tuple2<String, Set<Tuple2<String, Double>>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, Double>>> link)
					throws Exception {
				// TODO Auto-generated method stub
				return (link._2.size() > 1);
			}
        }).persist(StorageLevel.MEMORY_AND_DISK_SER());
        
        JavaPairRDD<String, Set<Tuple2<String, Double>>> oneToOneLinks = 
        links.filter(new Function<Tuple2<String, Set<Tuple2<String, Double>>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, Double>>> link)
					throws Exception {
				// TODO Auto-generated method stub
				return (link._2.size() == 1);
			}
        }).persist(StorageLevel.MEMORY_AND_DISK_SER());
      
       
        oneToOneLinks.saveAsTextFile(args[6]+"/oneToOneLinks");
        oneToNLinks.saveAsTextFile(args[6]+"/oneToNLinks");*/
        //links.saveAsTextFile(args[6]);
        
       
        
		ctx.close();
	}
}

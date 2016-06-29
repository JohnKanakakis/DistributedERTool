package spark;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.statistics.BlockStatistics;


public class Controller {

	private static final String STATS_FILE = "/user/kanakakis/stats_file";
	public static Logger logger = LoggerFactory.getLogger(Controller.class);
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
        
        
       
        
    	SparkConf sparkConf = new SparkConf().setAppName("Controller");
    	
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
    	
		Broadcast<byte[]> b = ctx.broadcast(planBinary);
		Broadcast<String[]> c = ctx.broadcast(configParams);
		Broadcast<Double> t = ctx.broadcast(thres);
		Broadcast<byte[]> skb = ctx.broadcast(skbBinary);
		Broadcast<byte[]> tkb = ctx.broadcast(tkbBinary);
		
    /*	Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("textinputformat.record.delimiter", "\n");
        
        JavaRDD<String> records1 = DataReader.run(ctx.newAPIHadoopFile(args[2], 
        											 TextInputFormat.class, 
        											 LongWritable.class, 
        											 Text.class,
        											 conf));
        							
        JavaRDD<String> records2 = DataReader.run(ctx.newAPIHadoopFile(args[3], 
			 										TextInputFormat.class, 
	 												LongWritable.class, 
	 												Text.class,
	 												conf));*/
		JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> records1 = ctx.objectFile(args[2]);
																						 
		JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> records2 = ctx.objectFile(args[3]);
        
		
		JavaPairRDD<String, List<String>> resources1 = ResourceFilter.runWithPairs(records1,skb);
		JavaPairRDD<String, List<String>> resources2 = ResourceFilter.runWithPairs(records2,tkb);
		
        JavaPairRDD<String, List<String>> resources = resources1.union(resources2)
        														.setName("resources")
        														.persist(StorageLevel.MEMORY_AND_DISK_SER());
        
       
        
        //resources.saveAsTextFile(args[4]);
        JavaPairRDD<String, Tuple2<String, String>> resourceIndex = 
        		IndexCreator.run(resources,skb,tkb);
        
        resourceIndex = resourceIndex.persist(StorageLevel.MEMORY_AND_DISK_SER()).setName("resourceIndex");
        
        
        //BlockStatistics.filterOfSizeN(resourceIndex,1).coalesce(10).saveAsTextFile(args[5]);
        
        JavaPairRDD<Integer, Integer> blocksFreq = IndexCreator.getFrequencyOfBlocks(resourceIndex);
        													   //.persist(StorageLevel.MEMORY_AND_DISK_SER());
        List<Tuple2<Integer, Integer>> blockSizes = blocksFreq.collect();
        
        int optimalSize = BlockStatistics.getOptimalBlockSize(blockSizes);
        
       
        //ArrayList<String> result = new ArrayList<String>();
        //result.add("optimal size:"+optimalSize);
        //ctx.parallelize(result,1).saveAsTextFile(STATS_FILE);
		//blocksFreq.saveAsTextFile(args[5]);
        
       /* 
        ArrayList<String> BKVs = new ArrayList<String>(resourceIndex.keys().collect());
        
        Collections.sort(BKVs);
        
        int L = BKVs.size();
        int W = 50;
       
        HashMap<String,Set<Integer>> w_bkv = new  HashMap<String,Set<Integer>>();
        String bkv;
        Set<Integer> windows;
        for(int i = 0; i < Math.max(L-W+1,1); i++){
			for(int j = i; j < Math.min(i+W,L); j++){
				bkv = BKVs.get(j);
				if(w_bkv.containsKey(bkv)){
					windows = w_bkv.get(bkv);
				}else{
					windows = new HashSet<Integer>();
				}
				windows.add(i);
				w_bkv.put(bkv, windows);
			}
        }
        
        Broadcast<HashMap<String, Set<Integer>>> w_bkv_B = ctx.broadcast(w_bkv);

        JavaPairRDD<String, Tuple2<String, String>> blocks = BlocksCreator.createWindows(resourceIndex,w_bkv_B);
*/
  
        
        //JavaPairRDD<String, List<List<String>>> blocks = BlocksCreator.createBlocks(resourceIndex,resources);
        															 //.persist(StorageLevel.MEMORY_AND_DISK_SER());
        															 //.setName("blocks");;
        //blocks.count();
        //resourceIndex.unpersist();
        //resources.unpersist();
        
        //logger.info("blocks before = "+blocks.count());
        //blocks = BlocksCreator.purge(blocks, optimalSize);
        
        //logger.info("blocks after = "+blocks.count());
        
        //blocks.saveAsObjectFile(args[5]);
        
        
        //JavaPairRDD<String, List<Tuple2<String, Double>>> links = Linker.run(blocks, skb, tkb, b, c, t,optimalSize);
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
       /* links.saveAsTextFile(args[6]);
        
        ArrayList<String> result = new ArrayList<String>();
        result.add("optimal size:"+optimalSize);
        result.add("links = "+links.count());
        result.add("oneToNLinks "+oneToNLinks.count());
       
        */
        //ctx.parallelize(result,1).saveAsTextFile(STATS_FILE);
        
		ctx.close();
	}
}


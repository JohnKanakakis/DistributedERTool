package spark.linking;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.aksw.limes.core.execution.engine.ExecutionEngine;
import org.aksw.limes.core.execution.engine.ExecutionEngineFactory;
import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.cache.*;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.reader.IConfigurationReader;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.aksw.limes.core.io.mapping.Mapping;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;


public class LinkFunction<S, L> implements FlatMapFunction<Iterator<String>,L>, Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private MemoryCache sourceCache = new MemoryCache();
    private MemoryCache targetCache = new MemoryCache();
    
	
	private Broadcast<List<String>> targetB;
	private Accumulable<ArrayList<String>, String> acclinks;
	Broadcast<String> configFile_B;
	
	final static Logger logger = Logger.getLogger(LinkFunction.class);

	
	

	public void setTarget(Broadcast<List<String>> targetB){
		this.targetB = targetB;
	}
	
	
	public Iterable<L> call(Iterator<String> sourcePartition) throws Exception {
		
		// link sourcePartition with broadcast target
		// returns local links
		
		
			
		logger.info("start filling caches");
		long t1 = System.currentTimeMillis();
		int i = 0;
		while(sourcePartition.hasNext()){
			String quad = sourcePartition.next();
			String[] triples = quad.split(" ");
			sourceCache.addTriple(triples[0],triples[1],triples[2]);
			//logger.info("triple :"+sourcePartition.next());
			i++;
		}
	
		logger.info("source at node has " + i +"triples");
		  //String[] t = this.target.getValue();
		
		//Iterator<String> t = target.toLocalIterator();
		//Iterator<String> t = targetAll.iterator();
		List<String> tv = targetB.value();
		Iterator<String> t = tv.iterator();
		logger.info("target has "+tv.size());
		while(t.hasNext()){
			String quad = t.next();
			if(quad == null) continue;
			String[] triples = quad.split(" ");
			targetCache.addTriple(triples[0],triples[1],triples[2]);
		}
		
		
		long t2 = System.currentTimeMillis();
		logger.info("finish filling caches : "+(t2-t1)+" msecs aprox = "+(t2-t1)/1000+" secs");
		
		this.linkData();
		ArrayList<L> localLinks = new ArrayList<L>();
		return localLinks;
		
	}


	private void linkData(){
		
		XMLConfigurationReader reader = new XMLConfigurationReader();
		org.aksw.limes.core.io.config.Configuration config = null;
	    org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = null;
		try {
			config = new Configuration();
			hdfs = FileSystem.get(hdfsConf);
			Path configXMLPath = new Path(configFile_B.getValue());
			Path LIMES_DTD_XML = new Path("/user/kanakakis/limes.dtd");
			FSDataInputStream in = hdfs.open(configXMLPath);
			FSDataInputStream dtd = hdfs.open(LIMES_DTD_XML);
			//config = reader.validateAndRead(in,dtd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		logger.info("start execution");
		long t1 = System.currentTimeMillis();
		
		logger.info("config is "+config.toString());
        Rewriter rw = RewriterFactory.getRewriter("Default");
        LinkSpecification ls = new LinkSpecification(config.getMetricExpression(), config.getVerificationThreshold());
        LinkSpecification rwLs = rw.rewrite(ls);
        // 4.3. Planning
        logger.info("planning");
        IPlanner planner = ExecutionPlannerFactory.getPlanner(config.getExecutionPlan(), sourceCache, targetCache);
        assert planner != null;
        NestedPlan plan = planner.plan(rwLs);
        logger.info("execution");
        
        // 5. Execution
        ExecutionEngine engine = ExecutionEngineFactory.getEngine("Default", sourceCache, targetCache,
                config.getSourceInfo().getVar(), config.getTargetInfo().getVar());
        assert engine != null;
        
        Mapping verificationMapping = engine.execute(plan);
        Mapping acceptanceMapping = verificationMapping.getSubMap(config.getAcceptanceThreshold());
        
        long t2 = System.currentTimeMillis();
        logger.info("execution finished. It took "+ (t2-t1)/1000+" secs");
        //String outputFormat = config.getOutputFormat();
        //ISerializer output = SerializerFactory.getSerializer(outputFormat);
        //output.setPrefixes(config.getPrefixes());
        //output.writeToFile(verificationMapping, config.getVerificationRelation(), config.getVerificationFile());
        //output.writeToFile(acceptanceMapping, config.getAcceptanceRelation(), config.getAcceptanceFile());
    
	}
	public void setConfiguration(Broadcast<String> confB) {
		configFile_B = confB;
	}


	public void setAccLink(Accumulable<ArrayList<String>, String> acclinks) {
		// TODO Auto-generated method stub
		this.acclinks = acclinks;
	}


	

	
}

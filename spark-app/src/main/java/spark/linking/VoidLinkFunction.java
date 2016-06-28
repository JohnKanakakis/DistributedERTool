package spark.linking;

import java.io.IOException;
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
import org.aksw.limes.core.io.cache.MemoryCache;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.aksw.limes.core.io.mapping.Mapping;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulable;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

public class VoidLinkFunction implements VoidFunction<Iterator<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private MemoryCache sourceCache = new MemoryCache();
    private MemoryCache targetCache = new MemoryCache();
	private Broadcast<List<String>> targetB;
	private Broadcast<String> confB;
	private Accumulable<ArrayList<String>, String> acclinks;
	final static Logger logger = Logger.getLogger(VoidLinkFunction.class);
	
	@Override
	public void call(Iterator<String> sourcePartition) throws Exception {
		// TODO Auto-generated method stub
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
		
		this.link();
	}

	
	public void setTarget(Broadcast<List<String>> targetB){
		this.targetB = targetB;
	}
	
	public void setConfiguration(Broadcast<String> confB){
		this.confB = confB;
	}

	private void link(){
		XMLConfigurationReader reader = new XMLConfigurationReader();
		org.aksw.limes.core.io.config.Configuration config = null;
	    org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = null;
		try {
			config = new Configuration();
			hdfs = FileSystem.get(hdfsConf);
			Path configXMLPath = new Path(confB.getValue());
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
        
        ArrayList<String> localLinks = new ArrayList<String>();
        localLinks.add("link");
        this.acclinks.merge(localLinks);
        
        long t2 = System.currentTimeMillis();
        logger.info("execution finished. It took "+ (t2-t1)/1000+" secs");
	}


	public void setAccLink(Accumulable<ArrayList<String>, String> acclinks) {
		this.acclinks = acclinks;
		
	}
}
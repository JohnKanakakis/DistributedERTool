package tests;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.aksw.limes.core.execution.engine.ExecutionEngine;
import org.aksw.limes.core.execution.engine.ExecutionEngineFactory;
import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.plan.Plan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.cache.HybridCache;
import org.aksw.limes.core.io.cache.Instance;
import org.aksw.limes.core.io.cache.MemoryCache;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.aksw.limes.core.io.mapping.Mapping;
import org.aksw.limes.core.io.preprocessing.Preprocessor;
import org.aksw.limes.core.io.serializer.ISerializer;
import org.aksw.limes.core.io.serializer.SerializerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.broadcast.Broadcast;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.ParserAdapter;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;

import scala.Tuple2;
import spark.DataFormatter;
import spark.Linker;
import spark.Utils;
import spark.model.DatasetManager;

public class LIMES {

	public static void main(String[] args) throws IOException, SAXException {
		// TODO Auto-generated method stub
		
		/*System.out.println(RDF.type);
		String resource = "dfdfdfdfdfdf_d1";
		int pos = "dfdfdfdfdfdf_d1".lastIndexOf("_d")+"_d".length();
		int datasetId = Integer.parseInt(resource.substring(pos));
		System.out.println(datasetId);
		System.exit(0);*/
		
	/*	String literal = "\"Fouc\"^^\"aut\"^^<http://www.w3.org/2001/XMLSchema-datatypesstring>";
		System.out.println(DataFormatter.eliminateDataTypeFromLiteral(literal));
		System.exit(0);*/
		
		XMLConfigurationReader reader = new XMLConfigurationReader();
    	
		org.aksw.limes.core.io.config.Configuration config = null;
		
		InputStream configFile = FileUtils.openInputStream(new File("LIMES/config2.xml"));
		InputStream dtdFile = FileUtils.openInputStream(new File("LIMES/limes.dtd"));;
		config = reader.validateAndRead(configFile,dtdFile);
		
		
		byte[] skbBinary = Utils.serialize(config.getSourceInfo());
        byte[] tkbBinary = Utils.serialize(config.getTargetInfo());
        
		String s1 = "";
		String s2 = "";
		try {
			s1 = FileUtils.readFileToString(new File("LIMES/input.ttl"));
			s2 = FileUtils.readFileToString(new File("LIMES/input2.ttl"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		MemoryCache m1 = readTTL(s1,(KBInfo) Utils.deserialize(skbBinary));
		MemoryCache m2 = readTTL(s2,(KBInfo) Utils.deserialize(tkbBinary));
		
		
		
		System.out.println("start execution");
		long t1 = System.currentTimeMillis();
		
		//System.out.println("config is "+config.toString());
        Rewriter rw = RewriterFactory.getRewriter("Default");
        LinkSpecification ls = new LinkSpecification(config.getMetricExpression(), config.getVerificationThreshold());
        LinkSpecification rwLs = rw.rewrite(ls);
        // 4.3. Planning
       // System.out.println("planning");
        IPlanner planner = ExecutionPlannerFactory.getPlanner(config.getExecutionPlan(), m1, m2);
        assert planner != null;
        NestedPlan plan = planner.plan(rwLs);
       // System.out.println("plan is "+plan);
        
        //List<NestedPlan> subPlans = plan.getSubPlans();
        //byte[] subPlansBinary = Utils.serialize(subPlans);
        
        byte[] planBinary = Utils.serialize(plan);
        plan = (NestedPlan) Utils.deserialize(planBinary);
        
       // subPlans = (List<NestedPlan>) Utils.deserialize(subPlansBinary);
        //plan.setSubPlans(subPlans);
        
       // System.out.println("dplan = "+plan);
       // System.out.println("ls is "+rwLs.toString());
       
        //System.out.println(plan.getOperator());
        // 5. Execution
        ExecutionEngine engine = ExecutionEngineFactory.getEngine("Default", m1, m2,
                config.getSourceInfo().getVar(), config.getTargetInfo().getVar());
        assert engine != null;
        
        
        Mapping verificationMapping = engine.execute(plan);
        
        
        Mapping acceptanceMapping = verificationMapping.getSubMap(config.getAcceptanceThreshold());
        
        long t2 = System.currentTimeMillis();
        System.out.println("execution finished. It took "+ (t2-t1)/1000+" secs");
        
       // System.out.println("thres "+config.getAcceptanceThreshold());
        
        System.out.println("verification mappings!\n"+verificationMapping);
        
        System.out.println("accepted mappings!\n"+acceptanceMapping);
        
        HashMap<String, Double> targets;
        double thres;
        for(String source: verificationMapping.getMap().keySet()){
        	targets = verificationMapping.getMap().get(source);
        	for(String target: targets.keySet()){
        		thres = targets.get(target);
        		if(thres >= config.getAcceptanceThreshold())
        			System.out.println(source +"->"+target +"|"+thres);
        	}
        }
		/*for(String source : acceptanceMapping.getMap().keySet()){
			for(String target: acceptanceMapping.getMap().get(source).keySet()){
				System.out.println(source+" "+acceptanceMapping.getConfidence(source, target)+" "+target);
			}
		}*/
	
	}
	
	private static MemoryCache readTTL(String s, KBInfo kb){
		MemoryCache m = new MemoryCache();
		
		try {
			//in = new FileInputStream("input.ttl");
			RDFParser p = Rio.createParser(RDFFormat.TURTLE);
			
			ArrayList<Statement> myList = new ArrayList<Statement>();
			StatementCollector collector = new StatementCollector(myList);
			p.setRDFHandler(collector);
			
			
			InputStream in = new ByteArrayInputStream(s.getBytes());
			
			p.parse(in,"");
			
			System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
			Statement st = null;
			
			HashMap<String,List<String>> resources = new HashMap<String,List<String>>();
			String subject;
			String predicate;
			String object;
			List<String> POs;
			for(int i = 0 ; i < myList.size(); i++){
				st = myList.get(i);
				subject = st.getSubject().toString();
				predicate = st.getPredicate().toString();
				object = st.getObject().toString();
				if(resources.containsKey(subject)){
					POs = resources.get(subject);
				}else{
					POs = new ArrayList<String>();
				}
				POs.add(predicate);
				POs.add(object);
				resources.put(subject, POs);
			}
			
			ArrayList<String> flatResources = new ArrayList<String>();
			for(Entry<String,List<String>> entry : resources.entrySet()){
				flatResources.add("@@@");
				flatResources.add(entry.getKey());
				for(String str : entry.getValue()){
					flatResources.add(str);
				}
			}
			for(int i = 0; i < flatResources.size();i++)
				System.out.println(flatResources.get(i));
			
			//System.out.println(resources);
			for(int i = 0; i < flatResources.size()-2; i++){
				if(flatResources.get(i).equals("@@@")){
					//cnt++;
					subject = flatResources.get(i+1);
				  	//datasetId = DatasetManager.getDatasetIdOfResource(subject);
				  	
				  	int j = i+2;
				  	while(j < flatResources.size() && !flatResources.get(j).equals("@@@")  ){
				  		predicate = flatResources.get(j);
				  		
				  		if(kb.getProperties().contains(predicate)){
				  			object = DataFormatter.eliminateDataTypeFromLiteral(flatResources.get(j+1));
				    		if(kb.getFunctions().get(predicate).keySet().size() == 0){
								m.addTriple(subject, predicate, object);
				    		}
							else{
								for(String propertyDub : kb.getFunctions().get(predicate).keySet()) {
									String value = Preprocessor.process(object, kb.getFunctions().get(predicate).get(propertyDub));
									m.addTriple(subject, propertyDub, value);
								}
							}	
						}	
				  		j = j + 2;
				  	}
				}
			}
			System.out.println(m);
			//System.exit(0);
			System.out.println(kb.getProperties());
			HashSet<String> configProperties = new HashSet<String>(kb.getProperties());
			System.out.println("properties = "+configProperties);
			
			for(String sub : resources.keySet()){
				
				POs = resources.get(sub);
				String record = "";
				for(int i = 0; i < POs.size()-1; i = i+2){
					predicate = POs.get(i);
					object = POs.get(i+1);
				
					if(kb.getProperties().contains(predicate)){
						//System.out.println("examing property: "+st.getPredicate().toString());
						
						object = DataFormatter.eliminateDataTypeFromLiteral(object);
						//remove localization information, e.g. @en
						record+=(object.replace("\"", "")+" ");
						if(kb.getFunctions().get(predicate).keySet().size() == 0){
							m.addTriple(sub, predicate, object);
							
						}else{
							
							for (String propertyDub : kb.getFunctions().get(predicate).keySet()) {
								
								System.out.println("propertyDub is "+propertyDub);
								
								String value = Preprocessor.process(object, kb.getFunctions()
																			  .get(predicate)
																			  .get(propertyDub));
								
								System.out.println("processed value "+value);
								
								m.addTriple(sub, propertyDub, value);
								
							}
						}
						
						
					}
					
					
					else{
						//System.out.println("adding statement "+ st.toString());
						m.addTriple(sub, predicate, object);
					}
				}
				System.out.println("record to index = "+record);
				String[] tokens = record.split(" ");
				for(int i = 0; i < tokens.length; i++){
					if(tokens[i].length() > 3){
						tokens[i] = tokens[i].trim();
						System.out.println("token pair ("+ tokens[i]+","+sub+")");
						//t = new Tuple2<String,String>(tokens[i],r_id);
						//tokenPairs.add(t);
						//tokenPairs.add(new Tuple2<String,Tuple2<String,String>>(r_id,t));
					}
				}
				//System.out.println("record to index = "+record);
			}
		
		} catch (IOException | RDFParseException | RDFHandlerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println(m);
		return m;
	}

}

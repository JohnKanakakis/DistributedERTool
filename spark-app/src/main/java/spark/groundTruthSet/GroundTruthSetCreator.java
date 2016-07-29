package spark.groundTruthSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.aksw.limes.core.io.query.ModelRegistry;
import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;

import spark.Utils;

public class GroundTruthSetCreator {

	
	
	static Model sourceOutputModel = ModelFactory.createDefaultModel();
	static Model targetOutputModel = ModelFactory.createDefaultModel();
	static Model linksOutputModel = ModelFactory.createDefaultModel();
	
	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		HashMap<String,String> allLinks = new HashMap<String,String>();
		int fromYear = 1970;
		int toYear = 2016;
		for(int year = fromYear; year <= toYear; year++){
			System.out.println("----------------- "+year+" ----------------------");
				allLinks.putAll(link(year));
			
			System.out.println("###############################################");
			
		}
		System.out.println("total links "+allLinks.size());
		
		sourceOutputModel.write(new FileOutputStream(new File("source.nt")),"N-TRIPLES");
		targetOutputModel.write(new FileOutputStream(new File("target.nt")),"N-TRIPLES");
		linksOutputModel.write(new FileOutputStream(new File("links.nt")),"N-TRIPLES");
		//byte[] ser = Utils.serialize(allLinks);
		
		/*try {
			//FileUtils.writeByteArrayToFile(new File("links.ser"),ser);
		
			HashMap<String,String> map = (HashMap<String, String>) Utils.deserialize(FileUtils.readFileToByteArray(new File("links.ser")));
			System.out.println(map);
			System.out.println(map.size());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
	}

	
	public static HashMap<String,String> link(int year) throws FileNotFoundException{
		
		Model source = ModelFactory.createDefaultModel();
		Model target = ModelFactory.createDefaultModel();
		
		Property sp_identifier 	= source.createProperty("http://purl.org/dc/terms/identifier");
		Property sp_name 		= source.createProperty("http://www.eurocris.org/ontologies/cerif/1.3#name");
		Property sp_year 		= source.createProperty("http://lod.openaire.eu/vocab/year");
		Resource resultEntity	= source.createResource("http://www.eurocris.org/ontologies/cerif/1.3#ResultEntity");
		
		
		Property tp_identifier 	= target.createProperty("http://purl.org/dc/terms/identifier");
		Property tp_name 		= target.createProperty("http://www.w3.org/2000/01/rdf-schema#label");
		Property tp_year 		= target.createProperty("http://purl.org/dc/terms/issued");
		Resource foafDocument	= target.createResource("http://swrc.ontoware.org/ontology#Article");
		
		String dblpQuery = 
				"PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"
				+"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"
				+ "PREFIX swrc: <http://swrc.ontoware.org/ontology#>\n"
				+"PREFIX dc: <http://purl.org/dc/elements/1.1/> \n"
				+ "PREFIX dcterms: <http://purl.org/dc/terms/> \n"		
				+"SELECT DISTINCT ?x ?name ?doi ?year \n"
				+ "WHERE {\n"
				+ "?x a swrc:Article.\n"
				+ "?x rdfs:label ?name. \n"
				+ "?x dc:identifier ?doi. \n"
				+ "?x dcterms:issued ?year. \n"
				+ "FILTER regex(?doi,\"DOI\"). \n"
				+ "FILTER(?year = \""+year+"\"). \n"
				+ "}\n";
		
		/*String openaireQuery1 = "select distinct ?x ?i ?name "
				+ "where { "
				+ "?x a <http://www.eurocris.org/ontologies/cerif/1.3#ResultEntity>. "
				+ "?x <http://purl.org/dc/terms/identifier> ?i. "
				+ "?x <http://www.eurocris.org/ontologies/cerif/1.3#name> ?name. "
				+ "FILTER(!regex(?i,\"[A-Z]\",\"i\"))"
				+ "} "
				+ "#LIMIT 1000";*/
		
		/*String openaireQuery2 = "construct {"
				+ "?x  a <http://www.eurocris.org/ontologies/cerif/1.3#ResultEntity>."
				+ "?x <http://www.eurocris.org/ontologies/cerif/1.3#name> ?name."
				+ "?x <http://purl.org/dc/terms/identifier> ?doi."
				+ "?x <http://lod.openaire.eu/vocab/year> ?year. \n"
				+ "} where { \n"
				+ "?x a <http://www.eurocris.org/ontologies/cerif/1.3#ResultEntity>. \n"
				+ "?x <http://purl.org/dc/terms/identifier> ?i. \n"
				+ "?x <http://www.eurocris.org/ontologies/cerif/1.3#name> ?name. \n"
				+ "?x <http://lod.openaire.eu/vocab/year> ?year.\n"
				+ "?x <http://lod.openaire.eu/vocab/resultType> \"publication\".\n"
				+ "FILTER(!regex(?i,\"[A-Z]\",\"i\") && ?year = \""+year+"\").\n"
				+ "BIND(replace(?i,' ','-') as ?doi)"
				+ "}\n";*/
		String openaireQuery2 = "select distinct ?x ?name ?year (replace(?i,' ','-') as ?doi)"
				+ "where { \n"
				+ "?x a <http://www.eurocris.org/ontologies/cerif/1.3#ResultEntity>. \n"
				+ "?x <http://purl.org/dc/terms/identifier> ?i. \n"
				+ "?x <http://www.eurocris.org/ontologies/cerif/1.3#name> ?name. \n"
				+ "?x <http://lod.openaire.eu/vocab/year> ?year.\n"
				+ "?x <http://lod.openaire.eu/vocab/resultType> \"publication\".\n"
				+ "FILTER(!regex(?i,\"[A-Z]\",\"i\") && ?year = \""+year+"\").\n"
				+ "#BIND()\n"
				+ "}\n";
				
				
		
		QueryExecution qexecOA = QueryExecutionFactory.sparqlService("https://virtuoso-beta.openaire.eu/sparql", openaireQuery2);
		
		QueryExecution qexecDBLP = QueryExecutionFactory.sparqlService("http://dblp.l3s.de/d2r/sparql", dblpQuery);
	
		ResultSet resultOA = qexecOA.execSelect();
		
		
		HashMap<String,String> OA_DOIs = new HashMap<String,String>();
		//HashMap<String,String> OA_names = new HashMap<String,String>();
		while(resultOA.hasNext()){
			QuerySolution record = resultOA.next();
			Resource resource = record.getResource("x");
			source.add(resource,RDF.type,resultEntity);
			
			Iterator<String> varnames = record.varNames();
			String o;
			while(varnames.hasNext()){
				String varName = varnames.next();
				//System.out.println(varName);
				if(varName.equals("doi")){
					o = record.get(varName).asLiteral().getValue().toString();
					source.add(resource,sp_identifier,o);
					OA_DOIs.put(o,resource.toString());
				}
				if(varName.equals("name")){
					o = record.get(varName).asLiteral().getValue().toString();
					source.add(resource,sp_name,o);
					//OA_names.put(record.get(varName).asLiteral().getValue().toString(),resource);
				}
				if(varName.equals("year")){
					o = record.get(varName).asLiteral().getValue().toString();
					source.add(resource,sp_year,o);
				}
			}
		}
		
		
		ResultSet resultDBLP = qexecDBLP.execSelect();
		
		HashMap<String,String> DBLP_DOIs = new HashMap<String,String>();
		HashMap<String,String> DBLP_names = new HashMap<String,String>();
		while(resultDBLP.hasNext()){
			QuerySolution record = resultDBLP.next();
			Resource resource = record.getResource("x");
			target.add(resource,RDF.type,foafDocument);
			
			Iterator<String> varnames = record.varNames();
			String o;
			while(varnames.hasNext()){
				String varName = varnames.next();
				
				if(varName.equals("doi")){
					o = record.get(varName).asLiteral().getValue().toString();
					target.add(resource,tp_identifier,o);
					DBLP_DOIs.put(o,resource.toString());
				}
				if(varName.equals("name")){
					o = record.get(varName).asLiteral().getValue().toString();
					target.add(resource,tp_name,o);
				}
				if(varName.equals("year")){
					o = record.get(varName).asLiteral().getValue().toString();
					target.add(resource,tp_year,o);
				}
			}
		}
		HashMap<String,String> links = new HashMap<String,String>();
		int cnt = 0;
		String source_uri;
		String target_uri;
		for(String DOI : DBLP_DOIs.keySet()){
			target_uri = DBLP_DOIs.get(DOI);
			try {
				String decodedDOI = URLDecoder.decode(DOI.substring(4), "UTF-8");
				if(OA_DOIs.containsKey(decodedDOI)){
					source_uri = OA_DOIs.get(decodedDOI);
					links.put(OA_DOIs.get(decodedDOI),DBLP_DOIs.get(DOI));
					System.out.println("link found with doi "+decodedDOI);
					linksOutputModel.add(source.getResource(source_uri),OWL.sameAs,target.getResource(target_uri));
					cnt++;
				}
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		/*for(String dblp_name : DBLP_names.keySet()){
			for(String oa_name : OA_names.keySet()){
				if(dblp_name.substring(0,dblp_name.length()-1).equals(oa_name)){
					String dblp_resource = DBLP_names.get(dblp_name);
					if(!links.containsKey(dblp_resource)){
						links.put(dblp_resource,OA_names.get(oa_name));
						System.out.println("new link found: ("+oa_name+","+dblp_name+")");
					}else{
						//System.out.println("already link found: ("+oa_name+","+dblp_name+")");
					}
					
				}
			}
		}*/
		
		System.out.println("oa dois "+OA_DOIs.size());
		System.out.println("dblp dois "+DBLP_DOIs.size());
		System.out.println(links);
		System.out.println(links.size());
		System.out.println(cnt);
		
		
		
	
		for(String uri : links.keySet()){
			Resource r = source.getResource(uri);
			
			sourceOutputModel.add(source.getProperty(r, sp_year));
			sourceOutputModel.add(source.getProperty(r, sp_identifier));
			sourceOutputModel.add(source.getProperty(r, sp_name));
			sourceOutputModel.add(source.getProperty(r, RDF.type));
			
		}
		
		for(String uri : links.values()){
			Resource r = source.getResource(uri);
			
			targetOutputModel.add(target.getProperty(r, tp_year));
			targetOutputModel.add(target.getProperty(r, tp_identifier));
			targetOutputModel.add(target.getProperty(r, tp_name));
			targetOutputModel.add(target.getProperty(r, RDF.type));
			
		}
		//noise
		ResIterator subjects = source.listSubjects();
		int totalNoisyResources = 1000; 
		while(subjects.hasNext() && totalNoisyResources > 0){
			Resource r = subjects.next();
			if(links.containsKey(r.toString())) continue;
			
			sourceOutputModel.add(source.getProperty(r, sp_year));
			sourceOutputModel.add(source.getProperty(r, sp_identifier));
			sourceOutputModel.add(source.getProperty(r, sp_name));
			sourceOutputModel.add(source.getProperty(r, RDF.type));
			totalNoisyResources--;
		}
	 
		subjects = target.listSubjects();
		totalNoisyResources = 1000; 
		while(subjects.hasNext() && totalNoisyResources > 0){
			Resource r = subjects.next();
			if(links.containsValue(r.toString())) continue;
			
			targetOutputModel.add(target.getProperty(r, tp_year));
			targetOutputModel.add(target.getProperty(r, tp_identifier));
			targetOutputModel.add(target.getProperty(r, tp_name));
			targetOutputModel.add(target.getProperty(r, RDF.type));
			totalNoisyResources--;
		}
		
		return links;
	}

}

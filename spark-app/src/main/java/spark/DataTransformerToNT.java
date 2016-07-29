package spark;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.MalformedInputException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.storage.StorageLevel;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashBigSet;
import scala.Tuple2;
import spark.io.CustomNTriplesParser;
import spark.io.DataReader;


public class DataTransformerToNT {

	public static Logger logger = LoggerFactory.getLogger(DataTransformerToNT.class);
	public final static org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
	public static FileSystem hdfs;
	
	
	public static void main(String[] args){
/*
		InputStream in;
		try {
			in = new FileInputStream(new File("/home/user/Downloads/OA_dump_000054.nt.gz"));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			ArrayList<String> buffer = new ArrayList<String>(3);
			while((line = reader.readLine()) != null){
				System.out.println("line : "+line);
				Text t = new Text(line);
				List<String> triple = toTriple(t,buffer);
				if(triple != null){
					System.out.println("it should be null");
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		
		System.exit(0);*/
			
			
		
		
		SparkConf sparkConf = new SparkConf().setAppName("Datatransformer");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
		
		hdfsConf.set("textinputformat.record.delimiter", "\n");
        
        //JavaPairRDD<String, Tuple2<String, String>> triples =
		int partitions = 1000;
		if(args.length == 3){
			partitions = Integer.parseInt(args[2]);
		}
		 
		
        JavaPairRDD<LongWritable, Text> data = ctx
        .newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class,hdfsConf);
        //.persist(StorageLevel.DISK_ONLY());
        
        //data.first();
        
        data.mapToPair(
        		new PairFunction<Tuple2<LongWritable,Text>,String, Tuple2<String,String>>(){

        			private static final long serialVersionUID = 1L;

        			private  List<String> toTriple(Text t,List<String> buffer){
        				
        				byte[] b = t.getBytes();
        				int pos1 = t.find("<");
        				
        				if(pos1 != 0) return null;
        				
        				int pos2 = t.find(">",pos1+1);
        				
        				if(pos2 == -1) return null;
        				
        				String s = null;
        				try {
        					s = Text.decode(b, pos1+1, pos2-pos1-1);
        				} catch (CharacterCodingException e) {
        					// TODO Auto-generated catch block
        					//e.printStackTrace();
        					return null;
        				}
        				
        				pos1 = t.find("<",pos2+1);
        				if(pos1 == -1) return null;
        				
        				pos2 = t.find(">",pos1+1);
        				if(pos2 == -1) return null;
        				
        				String p = null;
        				try {
        					p = Text.decode(b, pos1+1, pos2-pos1-1);
        				} catch (CharacterCodingException e) {
        					
        					return null;
        				}
        				
        				
        				String o = null;
        				pos1 = pos2;
        				while(t.charAt(pos1) != 9 && t.charAt(pos1) != ' ' && pos1 < t.getLength()){// 9 equals to tab
        					pos1++;
        				}
        				pos1++;
        				
        				if(pos1 >= t.getLength())
        					return null;
        				
        				if(t.charAt(pos1) == 34){// 34 equals to "\""
        					try {
        						o = Text.decode(b, pos1,t.getLength()-pos1-2);
        					} catch (CharacterCodingException e) {
        						return null;
        					}
        					
        				}else if (t.charAt(pos1) == 60){// 60 equals to "<"
        					//object is URI
        					pos2 = t.find(">",pos1+1);
        					if(pos2 == -1){
        						return null;
        					}
        					try {
        						o = Text.decode(b, pos1+1,pos2 - pos1 -1);
        					} catch (CharacterCodingException e) {
        						return null;
        					}
        					
        				}else{
        					return null;
        				}
        				
        				if(s != null && p != null && o != null){
        					buffer.add(s);
        					buffer.add(p);
        					buffer.add(o);
        					return buffer;
        				}else
        					return null;
        			}
        			
        			@Override
        			public Tuple2<String, Tuple2<String, String>> call(Tuple2<LongWritable,Text> record) throws Exception {
        				// TODO Auto-generated method stub
        				/*ArrayList<Tuple2<String,Tuple2<String,String>>> r = 
        						new ArrayList<Tuple2<String,Tuple2<String,String>>>();*/
        				//Text record;
        				List<String> triple;
        				ArrayList<String> buffer = new ArrayList<String>(3);
        				//while(records.hasNext()){
        					//record = records.next()._2;
        					
        					//logger.info("record:"+record);
        					//if(record.find("<") != 0) continue;
        					triple = toTriple(record._2, buffer);
        					if(triple == null){
        						logger.error("invalid triple "+record._2.charAt(0));
        						//System.exit(0);
        						//buffer.clear();
        						//continue;
        						return new Tuple2<String,Tuple2<String,String>>("",
            							new Tuple2<String,String>("",""));
        					}
        					if(triple.size() != 3){
        						logger.error("malformed triple "+record._2);
        						//System.exit(0);
        						//buffer.clear();
        						return new Tuple2<String,Tuple2<String,String>>("",
            							new Tuple2<String,String>("",""));
        						//continue;
        					}
        					
        					/*r.add(new Tuple2<String,Tuple2<String,String>>(triple.get(0),
        							new Tuple2<String,String>(triple.get(1),triple.get(2))));*/
        					//buffer.clear();
        				//}
        				
        				return new Tuple2<String,Tuple2<String,String>>(triple.get(0),
    							new Tuple2<String,String>(triple.get(1),triple.get(2)));
        			}
           
                })
        .filter(new Function<Tuple2<String,Tuple2<String,String>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<String, String>> resource) throws Exception {
				// TODO Auto-generated method stub
				return !resource._1.equals("");
			}
        })
        .aggregateByKey(new HashSet<Tuple2<String,String>>(), partitions,
	  		  	  new Function2<Set<Tuple2<String,String>>,Tuple2<String,String>,Set<Tuple2<String,String>>>(){
						private static final long serialVersionUID = 1L;
						@Override
						public Set<Tuple2<String,String>> call(Set<Tuple2<String,String>> set,Tuple2<String,String> po) 
						throws Exception {
							// TODO Auto-generated method stub
							if(po != null){
								set.add(po);
							}
							
							return set;
						}
				  }, 
	  		  	  new Function2<Set<Tuple2<String,String>>,Set<Tuple2<String,String>>,Set<Tuple2<String,String>>>(){

						private static final long serialVersionUID = 1L;
						@Override
						public Set<Tuple2<String,String>> call(Set<Tuple2<String,String>> set1,
												Set<Tuple2<String,String>> set2) 
						throws Exception {
							// TODO Auto-generated method stub
							set1.addAll(set2);
							return set1;
						}
				  })
        .saveAsObjectFile(args[1]);
        ctx.close();
	}
	
	
	
	public static void main1(String[] args) {
		
		
		
		
		SparkConf sparkConf = new SparkConf().setAppName("Datatransformer");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
    	
    	/*JavaPairRDD<String, PortableDataStream> zipFiles = ctx.binaryFiles(args[0],200)
    														  .persist(StorageLevel.DISK_ONLY());
    	HashPartitioner hp = new HashPartitioner(800);
    	
    	JavaRDD<byte[]> binaryFiles = zipFiles.map(new Function<Tuple2<String,PortableDataStream>,byte[]>(){

			private static final long serialVersionUID = 1L;

			@Override
			public byte[] call(Tuple2<String,PortableDataStream> zippedFile) throws Exception {
				// TODO Auto-generated method stub
				//ObjectBigArrayBigList<Statement> statements = new ObjectBigArrayBigList<Statement>();
				
				
				//MyRDFHandler handler = new MyRDFHandler();
				//StatementCollector collector = new StatementCollector(statements);
				
				//CustomNTriplesParser parser = new CustomNTriplesParser();
				
				//byte[] buffer = new byte[5096];
				//List<String> t = new ArrayList<String>(3);
				Tuple2<String,PortableDataStream> p;
				//while(zippedFiles.hasNext()){
					p = zippedFile;
					logger.info("extracting file "+ p._1);
					DataInputStream stream = p._2.open();
					
					GZIPInputStream zis = new GZIPInputStream(stream);
					
					//ByteArrayOutputStream out = new ByteArrayOutputStream(Integer.MAX_VALUE);//hdfs.create(new Path(outputFile), true, 1024);
					byte[] bytes = ByteStreams.toByteArray(zis);
			        int len;
			        while ((len = zis.read(buffer)) > 0) {
			        	out.write(buffer, 0, len);
			        }
			        stream.close();
			        zis.close();
			        //out.close();
			        zis = null;
			        stream = null;
			        
			        return bytes;
			        //
					
					
					//parser.setRDFHandler(handler);
					try{
						parser.parse(zis, "");
					}catch (RDFParseException e){
						zis.close();
						return parser.getResult();
					}
					zis = null;
					
					//logger.info("finished extracting file "+ p._1);
					//result.addAll((Collection<? extends Tuple2<String, Tuple2<String, String>>>) parser.getResult());
					
				//}
				
				
				Statement st = null;
				Tuple2<String,String> po = null;
				String s = null;
				String p = null;
				String o = null;
				for(int i = 0 ; i < statements.size64(); i++){
					st = statements.get(i);
					s = st.getSubject().toString();
					p = st.getPredicate().toString();
					o = st.getObject().toString();
					po = new Tuple2<String,String>(p,o);
					result.add(new Tuple2<String,Tuple2<String,String>>(s,po));
				}
				//return result;
			}
    	});*/
    	
    	/*JavaPairRDD<String, String> textFiles 
    	= binaryFiles.flatMapToPair(new PairFlatMapFunction<byte[],String, String>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, String>> call(byte[] bytes) throws Exception {
						// TODO Auto-generated method stub
						ObjectBigArrayBigList<Tuple2<String, String>> result = 
								new ObjectBigArrayBigList<Tuple2<String, String>>();
						String triple;
						String key;
						int pos1;
						int pos2;
						
						ByteArrayInputStream s = new ByteArrayInputStream(bytes);
						InputStreamReader reader = new InputStreamReader(s);
						BufferedReader br = new BufferedReader(reader);
						while( (triple = br.readLine()) != null){
							if(!triple.startsWith("<")){
								logger.warn("malformed input");
								break;
							}
							pos1 = triple.indexOf("<");
							pos2 = triple.indexOf(">");
							key = triple.substring(pos1, pos2);
							t = DataFormatter.toTriple(triple,t);
							if(t.get(0) == "" && t.get(1) == "" && t.get(2) == ""){
								continue;
							}
							// = new Tuple2<String,String>(t.get(1),t.get(2));
							result.add(new Tuple2<String,String>(key,triple.substring(pos2+1)));
							for(int i = 0; i < t.size(); i++){
								t.set(i,"");
							}
							triple = null;
						}
						s.close();
						br.close();
						reader.close();
						s = null;
						br = null;
						reader = null;
						return result;
					}  
    	}).persist(StorageLevel.DISK_ONLY());
    	textFiles.saveAsTextFile(args[1]);*/
    	//Configuration conf = new org.apache.hadoop.conf.Configuration();
        //conf.set("textinputformat.record.delimiter", "\n");
        
     
    	/*JavaPairRDD<String, PortableDataStream> zipFiles = ctx.binaryFiles(args[0],1600)
				  .persist(StorageLevel.DISK_ONLY());*/
    	
    	//JavaPairRDD<String, byte[]> data = 
    	
    	/*zipFiles.mapValues(new Function<PortableDataStream,ByteArrayOutputStream>(){

			private static final long serialVersionUID = 1L;

			@Override
			public ByteArrayOutputStream call(PortableDataStream fileStream) throws Exception {
				// TODO Auto-generated method stub
				DataInputStream stream = fileStream.open();
				
				GZIPInputStream zis = new GZIPInputStream(stream,1500000000);
				byte[] buffer = new byte[4096];
				
				ByteArrayOutputStream out = new ByteArrayOutputStream();//hdfs.create(new Path(outputFile), true, 1024);
				//byte[] bytes = ByteStreams.toByteArray(zis);
		        int len;
		        
		        while ((len = zis.read(buffer)) > 0) {
		        	
		        	out.write(buffer, 0, len);
		        }
		        
				return out;
			}
    	})*/
    	ctx.binaryFiles(args[0])
    	.flatMapToPair(new PairFlatMapFunction<Tuple2<String,PortableDataStream>,String,Tuple2<String,String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Tuple2<String,String>>> call(Tuple2<String,PortableDataStream> file) throws Exception {
				// TODO Auto-generated method stub
				CustomNTriplesParser parser = new CustomNTriplesParser();
				
				ObjectBigArrayBigList<Tuple2<String,Tuple2<String,String>>> result = 
						new ObjectBigArrayBigList<Tuple2<String,Tuple2<String,String>>>();
				
				//Tuple2<String, PortableDataStream> file;
				
				DataInputStream stream;
				
				//while(files.hasNext()){
					//file = files.next();
					
					if(file._1.contains("54")|| file._1.contains("55")||file._1.contains("107")||file._1.contains("108"))
						return result;
					
					stream = file._2.open();//new ByteArrayInputStream(file._2.toByteArray());
					//GZIPInputStream zis = new GZIPInputStream(stream);
					
					logger.info("parsing file "+file._1);
					try{
						parser.parse(stream, "");
					}catch (RDFParseException e){
						//zis.close();
						stream.close();
						stream = null;
						return result;
					}
					//zis.close();
					stream.close();
					stream = null;
					logger.info("finished parsing file "+file._1);
					result.addAll(parser.getResult());
				//}
				//parser = null;
				return result;
			}
        })
    	//.persist(StorageLevel.MEMORY_AND_DISK_SER())
    	.aggregateByKey(new HashSet<Tuple2<String,String>>(),500, 
			  		  	  new Function2<Set<Tuple2<String,String>>,Tuple2<String,String>,Set<Tuple2<String,String>>>(){
								private static final long serialVersionUID = 1L;
								@Override
								public Set<Tuple2<String,String>> call(Set<Tuple2<String,String>> set,Tuple2<String,String> po) 
								throws Exception {
									// TODO Auto-generated method stub
									set.add(po);
									return set;
								}
						  }, 
			  		  	  new Function2<Set<Tuple2<String,String>>,Set<Tuple2<String,String>>,Set<Tuple2<String,String>>>(){

								private static final long serialVersionUID = 1L;
								@Override
								public Set<Tuple2<String,String>> call(Set<Tuple2<String,String>> set1,
														Set<Tuple2<String,String>> set2) 
								throws Exception {
									// TODO Auto-generated method stub
									set1.addAll(set2);
									return set1;
								}
						  })
       /* .combineByKey(new Function<Tuple2<String,String>,String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
        	
        }, new Function2<String,String,String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String po1, String po2) throws Exception {
				// TODO Auto-generated method stub
				return po1 + DataFormatter.SEPERATOR + po2;
			}
        })*/
    	
    	.saveAsObjectFile(args[1]);
        
        
       // JavaRDD<Tuple2<String,ObjectOpenHashBigSet<Tuple2<String,String>>>> deserializedData = ctx.objectFile(args[1]);
        
        //deserializedData.saveAsTextFile(args[2]);
        
		ctx.close();
	}
	


/*class MyRDFHandler extends StatementCollector implements Serializable{

	*//**
	 * 
	 *//*
	private static final long serialVersionUID = 1L;
	ObjectBigArrayBigList<Tuple2<String,Tuple2<String, String>>> result = 
			new ObjectBigArrayBigList<Tuple2<String, Tuple2<String,String>>>();
	
	public void handleStatement(Statement st) {
		String s = st.getSubject().toString();
		String p = st.getPredicate().toString();
		String o = st.getObject().toString();
		Tuple2<String,String> po = new Tuple2<String,String>(p,o);
		result.add(new Tuple2<String,Tuple2<String,String>>(s,po));
	}
	
	public Iterable<Tuple2<String, Tuple2<String, String>>> getResult(){
		return result;
	}
}
*/

	public static void main2(String[] args) {
		// TODO Auto-generated method stub
		FileSystem hdfs = null;
    	List<Tuple2<String,String>> IOFilePairs= new ArrayList<Tuple2<String,String>>();
    	Path input = new Path(args[0]);
    	String outputDirectory = args[1];
		try {
			hdfs = FileSystem.get(hdfsConf);
			RemoteIterator<LocatedFileStatus> filesIterator = hdfs.listFiles(input, false);
			Tuple2<String,String> ioPair;
			
			
			String inputFile;
			String fileID;
			String fileName;
			Pattern p = Pattern.compile("(.*).gz");
			
			while(filesIterator.hasNext()){
				LocatedFileStatus fileStatus = filesIterator.next();
				inputFile = fileStatus.getPath().toString();
				fileName = fileStatus.getPath().getName().toString();
				Matcher matcher = p.matcher(fileName);
				matcher.find();
				fileID = matcher.group(1);
				ioPair = new Tuple2<String,String>(inputFile,
												   outputDirectory+"/"+fileID);
				IOFilePairs.add(ioPair);
				
			}
			logger.info(IOFilePairs.toString());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			
		}

		SparkConf sparkConf = new SparkConf().setAppName("DataTranformer");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
        JavaPairRDD<String, String> files = ctx.parallelizePairs(IOFilePairs,800);
        
        unzipFiles(files);
        parseFiles(files);
        /*.aggregateByKey(new HashSet<Tuple2<String,String>>(), 
        				  new Function2<HashSet<Tuple2<String,String>>,
        							   Tuple2<String,String>,
        							   HashSet<Tuple2<String,String>>>(){

							private static final long serialVersionUID = 1L;
							@Override
							public HashSet<Tuple2<String,String>> call(HashSet<Tuple2<String,String>> set,
																		  Tuple2<String, String> po) 
																  throws Exception {
								// TODO Auto-generated method stub
								set.add(po);
								//set.add(po._1+"@@@");
								//set.add(po._2+"@@@");
								return set;
							}

    					}, 
        				new Function2<HashSet<Tuple2<String,String>>,
        				HashSet<Tuple2<String,String>>,
        				HashSet<Tuple2<String,String>>>(){

							private static final long serialVersionUID = 1L;
							@Override
							public HashSet<Tuple2<String,String>> call(HashSet<Tuple2<String,String>> set1,
									HashSet<Tuple2<String,String>> set2) throws Exception {
								// TODO Auto-generated method stub
								set1.addAll(set2);
								return set1;
							}
        }).mapValues(new Function<HashSet<Tuple2<String,String>>,List<String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(HashSet<Tuple2<String, String>> poSet) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<String> poList = new ArrayList<String>();
				for(Tuple2<String,String> po:poSet){
					poList.add(po._1+"@@@");
					poList.add(po._2+"@@@");
				}
				return poList;
			}
        })*/
        //.saveAsTextFile(args[1]);
        
    	
    	
    	ctx.close();
	}

	private static void parseFiles(JavaPairRDD<String, String> files) {
		// TODO Auto-generated method stub
		
	}



	public static void unzipFiles(JavaPairRDD<String,String> files){
		
		files.foreach(
        		new VoidFunction<Tuple2<String,String>>(){
    				private static final long serialVersionUID = 1L;

					@Override
					public void  call(Tuple2<String,String> filePair) throws Exception {
						// TODO Auto-generated method stub
						logger.info("inputFile = "+filePair._1);
						logger.info("outputFile = "+filePair._2);
						
						FSDataInputStream in;// = new GZIPInputStream(stream.open());
						GZIPInputStream zis;
						ObjectBigArrayBigList<Tuple2<String,Tuple2<String, String>>> result = 
								new ObjectBigArrayBigList<Tuple2<String, Tuple2<String,String>>>();
						try {

							hdfs = FileSystem.get(hdfsConf);
							in = hdfs.open(new Path(filePair._1));
							byte[] buffer = new byte[1024];
						
							zis = new GZIPInputStream(in);
							FSDataOutputStream out = hdfs.create(new Path(filePair._2), true, 1024);
							
					        int len;
					        while ((len = zis.read(buffer)) > 0) {
					        	out.write(buffer, 0, len);
					        }
					 
					        zis.close();
					    	out.close();
							
							/*Statement st = null;
							Tuple2<String,String> po = null;
							String s = null;
							String p = null;
							String o = null;
							for(int i = 0 ; i < statements.size64(); i++){
								st = statements.get(i);
								s = st.getSubject().toString();
								p = st.getPredicate().toString();
								o = st.getObject().toString();
								po = new Tuple2<String,String>(p,o);
								result.add(new Tuple2<String,Tuple2<String,String>>(s,po));
							}*/
							
						} catch (IOException e){
							logger.error("IO Exception error!!!");
							hdfs.close();
						}
								
						catch( RDFParseException | RDFHandlerException | IllegalArgumentException e) {
							// TODO Auto-generated catch block
							//in.close();
							//hdfs.close();
							e.printStackTrace();
							hdfs.close();
							//return parser.getResult();
						}
						//return parser.getResult();
					}
        });
	}
}

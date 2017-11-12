// lmokada Laxmikant Kishor Mokadam
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/*
 * Main class of the TFIDF Spark implementation.
 * Author: Tyler Stocksdale
 * Date:   10/31/2017
 */
public class TFIDF {

	static boolean DEBUG = true;

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setAppName("TFIDF");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data
		// Output is: ( filePath , fileContents ) for each file in inputPath
		String inputPath = args[0];
		JavaPairRDD<String,String> filesRDD = sc.wholeTextFiles(inputPath);
		
		// Get/set the number of documents (to be used in the IDF job)
		long numDocs = filesRDD.count();
		
		//Print filesRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = filesRDD.collect();
			System.out.println("------Contents of filesRDD------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2.trim() + ")");
			}
			System.out.println("--------------------------------");
		}
		
		/* 
		 * Initial Job
		 * Creates initial JavaPairRDD from filesRDD
		 * Contains each word@document from the corpus and also attaches the document size for 
		 * later use
		 * 
		 * Input:  ( filePath , fileContents )
		 * Map:    ( (word@document) , docSize )
		 */
		JavaPairRDD<String,Integer> wordsRDD = filesRDD.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,String>,String,Integer>() {
				public Iterable<Tuple2<String,Integer>> call(Tuple2<String,String> x) {
					// Collect data attributes
					String[] filePath = x._1.split("/");
					String document = filePath[filePath.length-1];
					String fileContents = x._2;
					String[] words = fileContents.split("\\s+");
					int docSize = words.length;
					
					// Output to Arraylist
					ArrayList ret = new ArrayList();
					for(String word : words) {
						ret.add(new Tuple2(word.trim() + "@" + document, docSize));
					}
					return ret;
				}
			}
		);
		
		//Print wordsRDD contents
		 if (DEBUG) {
			List<Tuple2<String, Integer>> list = wordsRDD.collect();
			System.out.println("------Contents of wordsRDD------");
			for (Tuple2<String, Integer> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}		
		
	 	/* 
	 	 * TF Job (Word Count Job + Document Size Job)
	 	 * Gathers all data needed for TF calculation from wordsRDD
	 	 *
	 	 * Input:  ( (word@document) , docSize )
	 	 * Map:    ( (word@document) , (1/docSize) )
	 	 * Reduce: ( (word@document) , (wordCount/docSize) )
	 	 */
	 	 JavaPairRDD<String,String> tfRDD = wordsRDD.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,Integer>,String,String>() {
				public Iterable<Tuple2<String,String>> call(Tuple2<String,Integer> x) {
					// reformat the key and value as word@document and 1/docSize
					// adds into the ArrayList and return the ArrayList
					String key = x._1;
					String docSize = x._2.toString();
					String value = 1+"/"+docSize;
					ArrayList ret = new ArrayList();
					ret.add(new Tuple2(key,value));
					return ret;
				}
			}
		).reduceByKey(new Function2<String, String, String>(){
			public String call(String val1, String val2){
				// sums all the numerators of the values of a a particular key, 
				// returns the total occurance of word in the document 
				Integer val1_numerator = Integer.parseInt(val1.split("/")[0]);
				Integer val2_numerator = Integer.parseInt(val2.split("/")[0]);
				String newVal = (val1_numerator+val2_numerator)+"/"+val1.split("/")[1];
				return newVal;
			}
		});
		
	
	
	 	//Print tfRDD contents
		 if (DEBUG) {
	 	 	List<Tuple2<String, String>> list = tfRDD.collect();
	 	 	System.out.println("-------Contents of tfRDD--------");
	 	 	for (Tuple2<String, String> tuple : list) {
	 	 		System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
	 	 	}
	 	 	System.out.println("--------------------------------");
	 	 }
		 
	 	/*
	 	 * IDF Job
	 	 * Gathers all data needed for IDF calculation from tfRDD
	 	 *
	 	 * Input:  ( (word@document) , (wordCount/docSize) )
	 	 * Map:    ( word , (1/document) )
	 	 * Reduce: ( word , (numDocsWithWord/document1,document2...) )
	 	 * Map:    ( (word@document) , (numDocs/numDocsWithWord) )
	 	 */
	 	JavaPairRDD<String,String> idfRDD = tfRDD.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,String>,String,String>() {
				public Iterable<Tuple2<String,String>> call(Tuple2<String,String> x) {
					// This function just do the reformating of the key and value
					// creates the ArrayList and adds the Tuple in it 
					// and return the ArrayList
					String key = x._1.split("@")[0];
					String value = 1+"/"+x._1.split("@")[1];
					ArrayList ret = new ArrayList();
					ret.add(new Tuple2(key,value));
					return ret;
				}
			}
		).reduceByKey(new Function2<String, String, String>(){
				public String call(String val1, String val2){
					// This function reduces the key value by adding the numerators
					// and concatinating all the denominator strings as mentioned above
					Integer val1_numerator = Integer.parseInt(val1.split("/")[0]);
					Integer val2_numerator = Integer.parseInt(val2.split("/")[0]);
					String val1_denominator = val1.split("/")[1];
					String val2_denominator = val2.split("/")[1];
					String newVal = (val1_numerator+val2_numerator)+"/"+val1_denominator+","+val2_denominator;
					return newVal;
				}
		}).flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,String>,String,String>() {
				public Iterable<Tuple2<String,String>> call(Tuple2<String,String> x) {
					// This function splits the value to seperate oout the document array
					// the iterate over the document loop, concatinate the word , @ and document
					// create value using splitted numDocsWithWord and numDocs provided.
					String word = x._1;
					String numDocsWithWord = x._2.split("/")[0];
					String document_arr[] = x._2.split("/")[1].split(",");
					ArrayList ret = new ArrayList();
					for(String document : document_arr) {
						String key = word+"@"+document;
						String value = numDocs+"/"+numDocsWithWord;
						ret.add(new Tuple2(key,value));
					}
					return ret;
				}
			}
		);
	
	 	//Print idfRDD contents
	 	if (DEBUG) {
	 		List<Tuple2<String, String>> list = idfRDD.collect();
	 		System.out.println("-------Contents of idfRDD-------");
	 		for (Tuple2<String, String> tuple : list) {
	 			System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
	 		}
	 		System.out.println("--------------------------------");
	 	}
	
	 	/*
		 * TF * IDF Job
	 	 * Calculates final TFIDF value from tfRDD and idfRDD
	 	 *
	 	 * Input:  ( (word@document) , (wordCount/docSize) )          [from tfRDD]
	 	 * Map:    ( (word@document) , TF )
	 	 * 
	 	 * Input:  ( (word@document) , (numDocs/numDocsWithWord) )    [from idfRDD]
	 	 * Map:    ( (word@document) , IDF )
	 	 * 
	 	 * Union:  ( (word@document) , TF )  U  ( (word@document) , IDF )
	 	 * Reduce: ( (word@document) , TFIDF )
	 	 * Map:    ( (document@word) , TFIDF )
	 	 *
	 	 * where TF    = wordCount/docSize
	 	 * where IDF   = ln(numDocs/numDocsWithWord)
	 	 * where TFIDF = TF * IDF
	 	 */
	 	JavaPairRDD<String,Double> tfFinalRDD = tfRDD.mapToPair(
	 		new PairFunction<Tuple2<String,String>,String,Double>() {
	 			public Tuple2<String,Double> call(Tuple2<String,String> x) {
					// This function just calculate the TF from value.
					// It just splits wordCount and docSize, 
					// then calculate TF = wordCount/docSize. 
	 				double wordCount = Double.parseDouble(x._2.split("/")[0]);
	 				double docSize = Double.parseDouble(x._2.split("/")[1]);
	 				double TF = wordCount/docSize;
	 				return new Tuple2(x._1, TF);
	 			}
	 		}
	 	);
	
	 	JavaPairRDD<String,Double> idfFinalRDD = idfRDD.mapToPair(
	 		new PairFunction<Tuple2<String,String>,String,Double>() {
	 			public Tuple2<String,Double> call(Tuple2<String,String> x) {
					// This function just calculate the IDF from value.
					// It just splits numDocs and numDocsWithWord, 
					// then calculate IDF = log(numDocs/numDocsWithWord).
	 				Double numDocs = Double.parseDouble(x._2.split("/")[0]);
	 				Double numDocsWithWord = Double.parseDouble(x._2.split("/")[1]);
	 				Double IDF = Math.log((Double)numDocs/numDocsWithWord);
	 				return new Tuple2(x._1, IDF);
	 			}
	 		}
	 	);
	
	 	JavaPairRDD<String,Double> tfidfRDD = tfFinalRDD.union(idfFinalRDD).reduceByKey(new Function2<Double, Double, Double>(){
				public Double call(Double val1, Double val2){
					// This function just calculate the TFIDF from TF and IDF of the particular key.
					// then calculate TFIDF = TF*IDF.
	 				return val1*val2;
				}
		}).flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,Double>,String,Double>() {
				public Iterable<Tuple2<String,Double>> call(Tuple2<String,Double> x) {
					// This function just reformats the key from word@document)
					// to document@word
					String word = x._1.split("@")[0];
	 				String document = x._1.split("@")[1];
					String key = document+"@"+word;
					ArrayList ret = new ArrayList();
					ret.add(new Tuple2(key, x._2));
					return ret;
				}
			}
		);
		
	 	//Print tfidfRDD contents in sorted order
	 	Map<String, Double> sortedMap = new TreeMap<>();
	 	List<Tuple2<String, Double>> list = tfidfRDD.collect();
	 	for (Tuple2<String, Double> tuple : list) {
	 		sortedMap.put(tuple._1, tuple._2);
	 	}
	 	if(DEBUG) System.out.println("-------Contents of tfidfRDD-------");
	 	for (String key : sortedMap.keySet()) {
	 		System.out.println(key + "\t" + sortedMap.get(key));
	 	}
	 	if(DEBUG) System.out.println("--------------------------------");	 
	 }	
}
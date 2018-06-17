package com.wordcount.basic;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Word count of a document done using Spark. This configuration uses 20 different map functions and
 * uses a HashPartitioner to collect the same words all into one basket. Therefore, it is able to
 * use concurrent reducer nodes (4).
 */
public class WordCount
{
    public static void wordCount(String filename) {
        // configuration to use to interact with spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word count App");

        // create a java version of the spark context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load input data, which is a text file read main
        // the repartition breaks up the document into 20 segments for parallelism
        JavaRDD<String> input = sc.textFile( filename ).repartition(20);
        // split input string into words
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")));

        // transform the collection of words into pairs (word and 1). We do not use a combiner here
        JavaPairRDD<String, Integer> counts = words
                // lowercase, remove apostrophes, grammar and lowercase
                .map(p -> p.replaceAll("(')|(\\W)", "$1"))
                .map(r -> r.replaceAll("[^a-zA-Z ]", ""))
                .map(q -> q.toLowerCase())

                .mapToPair(t -> new Tuple2( t, 1 ) )
                .partitionBy(new HashPartitioner(4))
                // this is reducing that parallelism back to one
                .reduceByKey( (x, y) -> (int)x + (int)y )
                .coalesce(1);

        counts.saveAsTextFile("src/main/java/resources/output");
    }
    public static void main( String[] args )
    {
        wordCount( "src/main/java/resources/shakespeare.txt" );
    }
}

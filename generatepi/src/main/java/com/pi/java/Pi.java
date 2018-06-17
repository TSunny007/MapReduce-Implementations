package com.pi.java;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import sun.jvm.hotspot.utilities.Assert;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * A map/reduce program that estimates the value of Pi
 * The implementation is discussed below.
 *
 * Mapper:
 *   Generate points in a unit square
 *   and then count points inside/outside of the inscribed circle of the square.
 *
 * Reducer:
 *   Accumulate points inside/outside results from the mappers.
 *
 * Let numTotal = numInside + numOutside.
 * The fraction numInside/numTotal is a rational approximation of
 * the value (Area of the circle)/(Area of the square) = $I$,
 * where the area of the inscribed circle is Pi/4
 * and the area of unit square is 1.
 * Finally, the estimated value of Pi is 4(numInside/numTotal).
 */
public class Pi
{

    private static BigDecimal calculatePi(JavaSparkContext sc, long samplePoints) {
        Assert.that(samplePoints > 0, "Can't sample with size 0");
        Random random = new Random();
        // we limit parallelization to 20 nodes max
        Map<Boolean, Integer> counts = sc.parallelize(LongStream.range(0,samplePoints).boxed().collect(Collectors.toList()), (int)Math.min(20l, samplePoints))
        // Here we generate random numbers and make new pairs deciding whether they fall inside the circle or not.
        .mapToPair(id -> new Tuple2<Boolean, Integer>(Math.pow(random.nextDouble(),2) + Math.pow(random.nextDouble(),2) <= 1, 1))
        // this will partition into two groups of pair with key of (1) true or (2) false
        .partitionBy(new HashPartitioner(2))
        // add up all the values with the same key
        .reduceByKey((x,y) -> (int)x+ (int)y)
        // now we can combine results into one node
        .coalesce(1)
        .collectAsMap();
        // very rare (close to impossible), but getting with key true could result in null, that is why we use optional.
        return new BigDecimal(4 * Optional.ofNullable(counts.get(true)).orElse(0)).divide(new BigDecimal(samplePoints));
    }

    public static BigDecimal calculatePi(long samplePoints, int trials) {
        BigDecimal derivedValues = new BigDecimal(0);

        // configuration to use to interact with spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Matrix Vector Multiplication");
        // create a java version of the spark context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");

        for (int i =0; i < trials; i++) {
            BigDecimal derivedValue = calculatePi(sc,samplePoints);
            System.out.println(derivedValue);

            derivedValues = derivedValues.add(calculatePi(sc,samplePoints));
        }
        return derivedValues.divide(new BigDecimal(trials), MathContext.DECIMAL128);
    }

    public static void main( String[] args )
    {
        System.out.println("MapReduce derivation of Pi:\n" + calculatePi(1_000_000, 15).toString());
    }
}

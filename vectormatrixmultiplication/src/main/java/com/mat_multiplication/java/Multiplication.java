package com.mat_multiplication.java;

import com.google.common.collect.Table;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import sun.jvm.hotspot.utilities.Assert;

import java.util.*;
import java.util.stream.IntStream;


public class Multiplication
{
    /**
     * Matrix- Vector multiplication
     * This takes a matrix and a vector and uses mapreduce to calculate the dot product\
     * as explained in Mining of Massive Datasets:
     * http://infolab.stanford.edu/~ullman/mmds/ch2.pdf
     */
    private static Double[] MatrixVectorMultiplicationSpark (final Double[][] matrix, Double[] vector) {
        Assert.that(matrix[0].length == vector.length, "Matrix and vector dimensions do not match");

        List<Tuple3<Integer, Integer, Double>> matrixTuples = new ArrayList<>();
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[0].length; j++) {
                // we don't add tuples which would multiply out to 0 anyways
                if (matrix[i][j] != 0 || vector[j] == 0)
                matrixTuples.add(new Tuple3(i,j,matrix[i][j]));
            }
        }

        Double[] product = new Double[matrix.length];

        // configuration to use to interact with spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Matrix Vector Multiplication");
        // create a java version of the spark context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");
        JavaPairRDD<Integer, Double> counts = sc.parallelize(matrixTuples,vector.length)
        .mapToPair(matElement -> new Tuple2(matElement._1(), matrix[matElement._1()][matElement._2()] * vector[matElement._2()]))
        .reduceByKey((x,y) -> (double)x+(double)y)
        .coalesce(1);
        counts.saveAsTextFile("src/main/java/output1");

        return counts.sortByKey()
        .values().collect().toArray(product);
    }

    public static void main( String[] args ) {
         VectorMatrixMultiplication();

    }

    public static void VectorMatrixMultiplication() {
        int matrixRows = 20, matrixColumns = 40;

        Double[][] matrix = generateRandomMatrix(matrixRows,matrixColumns);
        Double[] vector = generateRandomVector(matrixColumns);

        vector[15] = 0.;
        vector[12] = 0.;

        System.out.println("Matrix: ");
        printGrid(matrix);
        System.out.println("Vector: ");
        printVector(ArrayUtils.toPrimitive(vector));
        System.out.println("\nReal Product: ");
        printVector(multiply(matrix, vector));
        System.out.println("\nMapreduce Product: ");
        printVector(ArrayUtils.toPrimitive(MatrixVectorMultiplicationSpark(matrix, vector)));
    }

    // Helpers

    public static void printGrid(Double[][] matrix)
    {
        for(int i = 0; i < matrix.length; i++)
        {
            for(int j = 0; j < matrix[0].length; j++)
            {
                System.out.printf("%.0f ", (double)matrix[i][j]);
            }
            System.out.println();
        }
    }

    public static void printVector(double[] vector)
    {
        for(int i = 0; i < vector.length; i++)
        {
            System.out.printf("%.0f ", (double)vector[i]);
        }
    }

    public static Double[][] generateRandomMatrix(int rows, int columns) {
        Double[][] matrix = new Double[rows][columns];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                matrix[i][j] = new Double(Math.round(Math.random()*10));
            }
        }
        return matrix;
    }

    public static Double[] generateRandomVector(int entries) {
        Double[] vector = new Double[entries];
        for (int i = 0; i < entries; i++) {
            vector[i] = new Double(Math.round(Math.random()*10));
        }
        return vector;
    }

    public static double[] multiply(Double[][] matrix, Double[] vector) {
        return Arrays.stream(matrix)
                .mapToDouble(row ->
                        IntStream.range(0, row.length)
                                .mapToDouble(col -> row[col] * vector[col])
                                .sum()
                ).toArray();
    }
}

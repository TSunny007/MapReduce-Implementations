package com.mat_multiplication.java;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import sun.jvm.hotspot.utilities.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private static Double[] MatrixMultiplicationSpark(final Double[][] lhs, Double[][] rhs) {
        Assert.that(lhs[0].length == rhs.length, "Matrix dimensions do not match");
        // configuration to use to interact with spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Matrix Vector Multiplication");
        // create a java version of the spark context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");

        // each Tuple is as follows:
        // An unique identifier of where the array is from.
        List<Tuple4<Integer, Integer, Integer, Double>> matrixValues = new ArrayList<>();
        for (int j = 0; j < lhs[0].length; j++) {
            for (int i = 0; i < lhs.length; i++) {
                // we don't add tuples which would multiply out to 0 anyways
                if (!lhs[i][j].equals(0))
                    matrixValues.add(new Tuple4(1,i,j,lhs[i][j]));
            }
        }

        for (int j = 0; j < rhs.length; j++) {
            for (int k = 0; k < rhs[0].length; k++) {
                // we don't add tuples which would multiply out to 0 anyways
                if (!rhs[j][k].equals(0))
                    matrixValues.add(new Tuple4(2,j,k,rhs[j][k]));
            }
        }

        //For each matrix element mij, produce the key value pair 􏰀j,(M,i,mij)􏰁.
        // ikewise, for each matrix element njk, produce the key value
        // pair 􏰀j,(N,k,njk)􏰁. Note that M and N in the values are not the matrices themselves.
        // Rather they are names of the matrices or
        // (as we mentioned for the similar Map function used for natural join) better,
        // a bit indicating whether the element comes from M or N.
        JavaPairRDD<Integer, Tuple3<Integer, Integer, Double>> firstMap = sc.parallelize(matrixValues,1)
                .mapToPair(tuple -> {
                    if (tuple._1().equals(1)) {
                        return new Tuple2<Integer, Tuple3<Integer, Integer, Double>>(tuple._3(), new Tuple3<>(tuple._1(),tuple._2(),tuple._4()));
                    } else {
                        return new Tuple2<Integer, Tuple3<Integer, Integer, Double>>(tuple._2(), new Tuple3<>(tuple._1(),tuple._3(),tuple._4()));
                    }
                });


        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Integer>, Double>> firstReduce = firstMap
                /*.partitionBy(new HashPartitioner(20))*/

                //Zero Val
                .aggregateByKey(new Tuple2<>(new Tuple2<Integer, Integer>(null,null),0.0),

                //Sequential Function
                        (aggValue, pairValue) -> {
                    if (pairValue._1().equals(1)) {
                        aggValue = new Tuple2<>(new Tuple2<>(pairValue._2(), aggValue._1()._2()),
                                aggValue._2().equals(0) ? pairValue._3():aggValue._2()*pairValue._3());
                    } else {
                        aggValue = new Tuple2<>(new Tuple2<>(aggValue._1()._1(), pairValue._2()),
                                aggValue._2().equals(0) ? pairValue._3():aggValue._2()*pairValue._3());
                    }
                    return aggValue;
                    },

                //Combine Function
                        (aggValue, aggValue2) -> {
                    Double newProduct = null;
                    if (aggValue._2() != null && aggValue2._2() != null) {
                        newProduct = aggValue._2()*aggValue2._2();
                    } else if (aggValue._2() == null) {
                        newProduct = aggValue._2();
                    } else if (aggValue2._2() == null) {
                        newProduct = aggValue2._2();
                    } else throw new Exception("Combine on aggregate has gone haywire");


                    aggValue = new Tuple2<>(new Tuple2<>(aggValue._1()._1() == null ? aggValue2._1()._1():aggValue._1()._1(),
                            aggValue._1()._2() == null ? aggValue2._1()._2():aggValue._1()._2()),
                            newProduct);
                    return aggValue;
                });
                /*.partitionBy(new HashPartitioner(20))
                .aggregateByKey(
                        new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(null,null), 0.0),
                        (currentValue, newPairValue) -> {
                    if (newPairValue._1() == 1) {
                        currentValue._1()._1
                    } ()
                })*/
                /*
                .reduceByKey((x,y) -> {
                    if (x._1().intValue() < y._1().intValue()) {
                        return new Tuple3<Integer, Integer, Double>(x._2(), y._2(), x._3()*y._3());
                    }
                    else return new Tuple3<>(y._2(), x._2(), x._3()*y._3());
                })
                .coalesce(1)*/;
        firstReduce.saveAsTextFile("src/main/java/output1");
        return null;
    }

    public static void main( String[] args ) {
        // VectorMatrixMultiplication();
        MatrixMatrixMultiplication();

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

    public static void MatrixMatrixMultiplication() {
        Double[][] lhs = generateRandomMatrix(2,2);
        Double[][] rhs = generateRandomMatrix(2,2);
        System.out.println("lhs: ");
        printGrid(lhs);
        System.out.println("rhs: ");
        printGrid(rhs);
        MatrixMultiplicationSpark(lhs,rhs);
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

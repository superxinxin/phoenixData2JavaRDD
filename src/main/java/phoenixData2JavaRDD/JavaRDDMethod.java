package phoenixData2JavaRDD;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class JavaRDDMethod
{
	public static void main(String[] args) throws InterruptedException
	{
		System.setProperty("hadoop.home.dir", "F:\\upload_jdk\\hadoop-2.7.6\\hadoop-2.7.6");
        JavaSparkContext sc = sparkConf();
//    	读入：1.从txt文件读成JavaRDD;
//		     2.从List<String>读成JavaRDD
        List<String> phoenixData = dataFromPhoenix();
        JavaRDD<String> dataRDD = Data2RDDMethod(sc, phoenixData);
     
        JavaRDD<String> wordRDD = dataRDD2WordRDDMethod(dataRDD);
        
        JavaPairRDD<String, Integer> wordCountRDD =  wordCountRDDMethod(wordRDD);
        //打印RDD内容
        wordRDDPrint(wordRDD);
        //打印RDD统计内容
        wordCountRDDPrint(wordCountRDD);
        //RDD内容存入List
        List<String> wordRDDResultList = wordRDD2ListMethod(wordRDD);
        //RDD统计内容存入Map
        Map<String, Integer> wordCountRDDResultMap = wordCountRDD2MapMethod(wordCountRDD);
        //RDD统计内容存入文件
//      wordCountRDD2FileMethod(wordCountRDD);

        //输出list和map内容
        for(String s : wordRDDResultList)
        {
        	System.out.println("wordRDDResultList: "+s);
        }
        
        Set<Map.Entry<String, Integer>> entrySet = wordCountRDDResultMap.entrySet();
        for(Map.Entry<String, Integer> entry : entrySet)
        {
        	System.out.println("wordCountRDDResultMap: "+entry.getKey()+" : "+entry.getValue());
        }
	}
	public static JavaSparkContext sparkConf()
	{
		SparkConf conf = new SparkConf();
        conf.setAppName("PhoenixData2JavaRDD");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
	}
//	读入：1.从txt文件读成JavaRDD;
//		 2.从List<String>读成JavaRDD
	public static JavaRDD<String> Data2RDDMethod(JavaSparkContext sc, List<String> phoenixData)
	{
//      JavaRDD<String> fileRDD = sc.textFile("F:\\inputTest.txt");
		
		 JavaRDD<String> DataRDD = sc.parallelize(phoenixData);
		return DataRDD;
	}
	public static JavaRDD<String> dataRDD2WordRDDMethod(JavaRDD<String> dataRDD)
	{
		JavaRDD<String> wordRDD = dataRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		return wordRDD;
	}
	public static JavaPairRDD<String, Integer> wordCountRDDMethod(JavaRDD<String> wordRDD)
	{
		JavaPairRDD<String, Integer> wordOneRDD = wordRDD.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCountRDD = wordOneRDD.reduceByKey((x, y) -> x + y);
        return wordCountRDD;
	}
	public static void wordRDDPrint(JavaRDD<String> wordRDD)
	{
		wordRDD.foreach(new VoidFunction<String>()
		{
			@Override
			public void call(String t) throws Exception
			{
				System.out.println("wordRDDPrint: "+t);
			}
		});
	}
	public static void wordCountRDDPrint(JavaPairRDD<String, Integer> wordCountRDD)
	{
		wordCountRDD.foreach(new VoidFunction<Tuple2<String,Integer>>()
		{
			@Override
			public void call(Tuple2<String, Integer> tuple2) 
			{
				System.out.println("wordCountRDDPrint: "+tuple2._1()+" : "+tuple2._2());
			}
		});
	}
	public static List<String> wordRDD2ListMethod(JavaRDD<String> wordRDD)
	{
		List<String> list = wordRDD.collect();
		return list;
	}
	public static Map<String, Integer> wordCountRDD2MapMethod(JavaPairRDD<String, Integer> wordCountRDD)
	{
		Map<String, Integer> map = wordCountRDD.collectAsMap();
		return map;
	}
	public static void wordCountRDD2FileMethod(JavaPairRDD<String, Integer> wordCountRDD)
	{
		wordCountRDD.saveAsTextFile("F:\\outputTest");
	}
	public static List<String> dataFromPhoenix()
	{
		List<String> data = new ArrayList<String>();
		String s0 = new String("a1,b2,c3,d4,e5,f6,g7");
		String s1 = new String("a3,b2,c3,d4,e2,f3,g5");
		String s2 = new String("a4,b3,c2,d3,e5,f6,g4");
		String s3 = new String("a1,b1,c4,d4,e1,f2,g7");
		String s4 = new String("a5,b3,c5,d2,e5,f1,g3");
		String s5 = new String("a3,b1,c3,d1,e1,f1,g7");
		String s6 = new String("a5,b4,c6,d3,e7,f6,g2");
		String s7 = new String("a7,b5,c6,d3,e4,f7,g3");
		String s8 = new String("a2,b7,c3,d4,e3,f5,g7");
		String s9 = new String("a3,b2,c3,d1,e2,f4,g1");
		data.add(s0);
		data.add(s1);
		data.add(s2);
		data.add(s3);
		data.add(s4);
		data.add(s5);
		data.add(s6);
		data.add(s7);
		data.add(s8);
		data.add(s9);
		return data;
	}
}
//JavaPairRDD<Integer, String> count2WordRDD = wordCountRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
//JavaPairRDD<Integer, String> sortRDD = count2WordRDD.sortByKey(false);
//JavaPairRDD<String, Integer> resultRDD = sortRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
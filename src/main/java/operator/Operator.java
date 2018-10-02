package operator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdfs.server.namenode.HostFileManager.EntrySet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class Operator
{
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "F:\\upload_jdk\\hadoop-2.7.6\\hadoop-2.7.6");
        JavaSparkContext sc = sparkConf();
        JavaRDD<String> dataRDD = Data2RDDMethod(sc);
        Map<String, String> filterFieldsAndValues = new HashMap<String, String>();
        filterFieldsAndValues.put("C", "c3");
        filterFieldsAndValues.put("G", "g7");
        List<String> selectFields = new ArrayList<String>();
        selectFields.add("B");
        selectFields.add("D");
        List<String> allField = new ArrayList<String>();
        allField.add("A");
        allField.add("B");
        allField.add("C");
        allField.add("D");
        allField.add("E");
        allField.add("F");
        allField.add("G");
        //全部数据
        dataRDD.foreach(new VoidFunction<String>() {

			@Override
			public void call(String t) throws Exception
			{
				System.out.println("全部数据："+t);
			}
        });
        //过滤算子
        JavaRDD<String> filterResult = filterMethod(filterFieldsAndValues, allField, dataRDD, false);
        filterResult.foreach(new VoidFunction<String>()
		{
			@Override
			public void call(String t) throws Exception
			{
				System.out.println("过滤算子： "+t);
			}
		});
        //统计算子
        Map<String, Integer> counterResult = CounterMethod(selectFields, allField, dataRDD);
        Set<Map.Entry<String, Integer>> entrySet = counterResult.entrySet();
        for(Map.Entry<String, Integer> en : entrySet)
        {
        	System.out.println("统计算子： "+en.getKey()+" : "+en.getValue());
        }
        //抽样算子
        Set<String> sampleResult = sampleMethod(selectFields, allField, dataRDD);
        for(String res : sampleResult)
        {
        	System.out.println("抽样算子： "+res);
        }
	}
	public static JavaRDD<String> filterMethod(Map<String, String> filterFieldsAndValues, List<String> allField, JavaRDD<String> dataRDD, boolean b)
	{
		List<String> filterFields = new ArrayList<String>(); 
		List<String> filterValues = new ArrayList<String>(); 
		List<Integer> indexList = new ArrayList<Integer>(); 
		Set<Map.Entry<String, String>> entrySet = filterFieldsAndValues.entrySet();
		for(Map.Entry<String, String> entry : entrySet)
		{
			filterFields.add(entry.getKey());
			filterValues.add(entry.getValue());
		}
		for(String field : filterFields)
		{
			int tmp = allField.indexOf(field);
			indexList.add(tmp);
		}
		JavaRDD<String> dataRDD2 = dataRDD;
		if(b == false)
		{
			for(int i=0; i<filterValues.size(); i++)
			{
				int idx1 = indexList.get(i);
				int idx2 = i;
				dataRDD2 = dataRDD2.filter(new Function<String,Boolean>(){
					@Override
					public Boolean call(String v1) throws Exception
					{
						String[] str = v1.split(",");
						if(str[idx1].equals(filterValues.get(idx2)))
						{
							return true;
						}
						else
						{
							return false;
						}
					}
		        });
			}
		}
		else
		{
			dataRDD2 = dataRDD2.filter(new Function<String,Boolean>(){
				@Override
				public Boolean call(String v1) throws Exception
				{
					boolean bb = true;
					boolean tmp = false;
					String[] str = v1.split(",");
					for(int i=0; i<filterValues.size(); i++)
					{
						if(str[indexList.get(i)].equals(filterValues.get(i)))
						{
							tmp = true;
						}
						else
						{
							tmp = false;
						}
						bb = tmp==bb;
						if(bb == false)
						{
							break;
						}
					}
					return !bb;
				}
	        });
		}
		return dataRDD2;
	}
	public static Map<String, Integer> CounterMethod(List<String> selectFields, List<String> allField, JavaRDD<String> dataRDD)
	{
//		JavaPairRDD<String,Integer> ddd = 
//				dataRDD.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey(((x, y) -> x + y));
		JavaPairRDD<String,Integer> ddd = dataRDD.mapToPair(new PairFunction<String,String,Integer>()
				{
					@Override
					public Tuple2<String, Integer> call(String t) throws Exception
					{
						List<Integer> indexList = new ArrayList<Integer>(); 
						for(String field : selectFields)
						{
							int tmp = allField.indexOf(field);
							indexList.add(tmp);
						}
						StringBuffer resk = new StringBuffer("");
						String[] str = t.split(",");
						for(int i : indexList)
						{
							String k = allField.get(i);
							String v = str[i];
							resk.append(k);
							resk.append(":");
							resk.append(v);
							resk.append(" ");
						}
						Tuple2<String, Integer> tup = new Tuple2<String,Integer>(resk.toString(),new Integer(1));
						return tup;
					}
				});
		JavaPairRDD<String,Integer> ddd2 = ddd.reduceByKey(((x, y) -> x + y));
		Map<String, Integer> map = ddd2.collectAsMap();
		return map;		
				
//		Map<String, Long> map = new HashMap<String, Long>();
//		List<Integer> indexList = new ArrayList<Integer>(); 
//		for(String field : selectFields)
//		{
//			int tmp = allField.indexOf(field);
//			indexList.add(tmp);
//		}
//		List<String> list = dataRDD.collect();
//		for(String s : list)
//		{
//			int index = 0;
//			String resk = "";
//			String[] str = s.split(",");
//			Map<String, String> filterMap = new HashMap<String, String>();
//			for(int i : indexList)
//			{
//				String k = selectFields.get(index++);
//				String v = str[i];
//				resk += k+":"+v+" ";
//				filterMap.put(k, v);
//			}
//			map.put(resk, filterMethod(filterMap, allField, dataRDD, false).count());
//		}
//		return map;
	}
	public static Set<String> sampleMethod(List<String> selectFields, List<String> allField, JavaRDD<String> dataRDD)
	{
		Set<String> resSet = new HashSet<String>();
		List<Integer> indexList = new ArrayList<Integer>(); 
		for(String field : selectFields)
		{
			int tmp = allField.indexOf(field);
			indexList.add(tmp);
		}
		List<String> list = dataRDD.collect();
		for(String s : list)
		{
			int index = 0;
			String resk = "";
			String[] str = s.split(",");
			Map<String, String> filterMap = new HashMap<String, String>();
			for(int i : indexList)
			{
				String k = selectFields.get(index++);
				String v = str[i];
				filterMap.put(k, v);
			}
			resSet.add(filterMethod(filterMap, allField, dataRDD, false).first());
		}
		return resSet;
	}
	public static JavaSparkContext sparkConf()
	{
		SparkConf conf = new SparkConf();
        conf.setAppName("PhoenixData2JavaRDD");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
	}
	public static JavaRDD<String> Data2RDDMethod(JavaSparkContext sc)
	{
//      JavaRDD<String> fileRDD = sc.textFile("F:\\inputTest.txt");
		List<String> phoenixData = dataFromPhoenix();
		 JavaRDD<String> DataRDD = sc.parallelize(phoenixData);
		return DataRDD;
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

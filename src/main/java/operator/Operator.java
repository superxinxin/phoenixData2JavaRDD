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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Operator
{
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "F:\\upload_jdk\\hadoop-2.7.6\\hadoop-2.7.6");
        JavaSparkContext sc = sparkConf();
        JavaRDD<String> dataRDD = Data2RDDMethod(sc);
        
        String[] selectValues = new String[]{"a3","b2"};
        String selectField = "G";
        String[] allField = new String[]{"A","B","C","D","E","F","G"};
        //过滤算子
        JavaRDD<String> filterResult = filterMethod(selectValues, dataRDD, true);
        filterResult.foreach(new VoidFunction<String>()
		{
			@Override
			public void call(String t) throws Exception
			{
				System.out.println("过滤算子： "+t);
			}
		});
        //统计算子
        Map<String, Long> counterResult = CounterMethod(selectField, allField, dataRDD);
        Set<Map.Entry<String, Long>> entrySet = counterResult.entrySet();
        for(Map.Entry<String, Long> en : entrySet)
        {
        	System.out.println("统计算子： "+en.getKey()+" : "+en.getValue());
        }
        //抽样算子
        List<String> sampleResult = sampleMethod(selectField, allField, dataRDD);
        for(String res : sampleResult)
        {
        	System.out.println("抽样算子： "+res);
        }
	}
	public static JavaRDD<String> filterMethod(String[] selectValues, JavaRDD<String> dataRDD, boolean b)
	{
		JavaRDD<String> dataRDD2 = dataRDD;
		if(b == false)
		{
			for(String s : selectValues)
			{
				dataRDD2 = dataRDD2.filter(new Function<String,Boolean>(){
					@Override
					public Boolean call(String v1) throws Exception
					{
							return v1.indexOf(s) != -1;
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
					for(String s : selectValues)
					{
						boolean tmp = v1.indexOf(s)!=-1;
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
	public static Map<String, Long> CounterMethod(String selectField, String[] allField, JavaRDD<String> dataRDD)
	{
		int index = -1;
		for(int i=0; i<allField.length; i++)
		{
			if(allField[i].equals(selectField))
			{
				index = i;
			}
		}
		if(index == -1)
		{
			return null;
		}
		else
		{
			Set<String> set = new HashSet<String>();
			Map<String, Long> map = new HashMap<String, Long>();
			List<String> list = dataRDD.collect();
			for(String tmp : list)
			{
				String[] str = tmp.split(",");
				set.add(str[index]);
			}
			for(String s: set)
			{
				String[] ss = s.split(",");
				JavaRDD<String> filterResult = filterMethod(ss, dataRDD, false);
				map.put(s, filterResult.count());
			}
			return map;
		}
	}
	public static List<String> sampleMethod(String selectField, String[] allField, JavaRDD<String> dataRDD)
	{
		int index = -1;
		for(int i=0; i<allField.length; i++)
		{
			if(allField[i].equals(selectField))
			{
				index = i;
			}
		}
		if(index == -1)
		{
			return null;
		}
		else
		{
			Set<String> set = new HashSet<String>();
			List<String> list = dataRDD.collect();
			List<String> list2 = new ArrayList<String>();
			for(String tmp : list)
			{
				String[] str = tmp.split(",");
				set.add(str[index]);
			}
			for(String s: set)
			{
				String[] ss = s.split(",");
				JavaRDD<String> filterResult = filterMethod(ss, dataRDD, false);
				list2.add(filterResult.first());
			}
			return list2;
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

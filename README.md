# phoenixData2JavaRDD
# Operator.java
## 数据格式：字符串类型，形如“a1，b2，c3，d4，e5”
## 过滤算子
    1）算子简介：对一条记录中某个或某几个字段的指定字段值进行过滤，过滤可以是过滤后去掉，也可以是过滤后留下。
    2）算子实现：filterFieldsAndValues是Map类型，键放字段，值放字段值，
       在filterMethod方法中可以设置形参true或false，true表示将指定字段含有指定字段值的记录过滤掉，
       false表示将指定字段含有指定字段值的记录留下，其他过滤掉。
## 统计算子
    1）算子简介：统计一个或多个指定字段含有相同字段值的记录条数
    2）算子实现：selectFields是ArrayList类型，存放指定字段，
       CounterMethod方法中根据指定字段得到字段值，统计指定字段含有相同字段值的记录条数
## 抽样算子
    1）算子简介：在指定字段含有相同字段值的记录中抽取一条
    2）算子实现：selectFields是ArrayList类型，存放指定字段，
    sampleMethod方法实现指定字段含有相同字段值的记录，只抽取其中一条。
## 实现过程及截图
* 1）全部数据![图1](https://github.com/superxinxin/phoenixData2JavaRDD/blob/master/Images/1.PNG)
* 2）设置过滤字段及字段值，设置统计字段和抽样字段![图2](https://github.com/superxinxin/phoenixData2JavaRDD/blob/master/Images/5.PNG)
* 3）过滤算子结果。提取出C是c3，G是g7的记录![图3](https://github.com/superxinxin/phoenixData2JavaRDD/blob/master/Images/2.PNG)
* 4）统计算子结果。根据字段B和D的字段值进行统计![图4](https://github.com/superxinxin/phoenixData2JavaRDD/blob/master/Images/3.PNG)
* 5）抽样算子结果。根据字段B和D的字段值进行统计![图5](https://github.com/superxinxin/phoenixData2JavaRDD/blob/master/Images/4.PNG)
# JavaRDDMethod.java
* 数据读入：1）从txt文件读成JavaRDD；2）从List<String>读成JavaRDD。
* RDD打印：1）打印RDD内容；2）打印WordCountRDD统计内容；
* RDD数据存储：1）RDD内容存入List；2）RDD统计内容存入Map；3）RDD统计内容存入文件。

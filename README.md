# MusicRecommend
一、基于SparkMLlib的音乐推荐示例
1、数据清洗

2、将rdd里面是数据从string转换成Rating(userID,productId,count)
  （既rdd【String】转化成rdd【Rating】）

3、将第二步生成的rdd扔到ALS的trainImplicit或train这个两个方法里面进行训练，加上一些必要的参数，如rank等
  参数的意思在代码中有解释，生成训练的模型
  
4、检测模型的AUC（具体看代码：实现方式有很多种）

二、基于kafka+sparkStreaming+mongodb的实时数据采集系统

（一）往kafka的topic里面写数据

1、创建java的properties -----主要存放两个参数：metadata.broker.list（brokerlist）和serializer.class（字符解析类）

2、创建ProducerConfig  -----将刚刚的properties放置于里面

3、创建producer -----new Producer【String，String】（ProducerConfig）

4、producer.send（new KeyedMessage[String,String](topics.toArray.apply(0),event.toString()）
   keyedMessage -------一个消息类（topics，message）
   topics       -------可以一个通道，也可以多个
   
（二）spark streaming与kafka对接接收来自kafka的数据

1、创建StreamingContext（老规矩，不详细说）

2、创建一个topicSet（存放要从哪个topic拿数据）




import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

 	/*贝叶斯文档分类器的多项式模型–以单词为粒度
 	条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/（类c下单词总数+训练样本中不重复的单词总数）
  	先验概率P(c)=类c下的单词总数/整个训练样本的单词总数*/

public class NaiveBayesBasedOnWord2 {
	static String[] otherArgs; 
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 7){
			System.err.println("error: Invalid Arguments!");
			System.exit(7);
		}
		
		FileSystem hdfs = FileSystem.get(conf);

		/*设置Job0*/
		Path path0 = new Path(otherArgs[6]);
		if(hdfs.exists(path0))
			hdfs.delete(path0, true);//如果MapReduce0的输出目录已存在，则删除
		Job job0 = new Job(conf, "job0-DocCounts");
		job0.setJarByClass(NaiveBayesBasedOnWord2.class);
		job0.setInputFormatClass(SequenceFileInputFormat.class);
		job0.setOutputFormatClass(SequenceFileOutputFormat.class);
		job0.setMapperClass(ClassTotalDocsMap.class);
		job0.setMapOutputKeyClass(Text.class);//map阶段的输出的key
		job0.setMapOutputValueClass(IntWritable.class);//map阶段的输出的value
		job0.setReducerClass(ClassTotalDocsReduce.class);
		job0.setOutputKeyClass(Text.class);//reduce阶段的输出的key
		job0.setOutputValueClass(IntWritable.class);//reduce阶段的输出的value
		//加入控制容器
		ControlledJob ctrljob0 = new  ControlledJob(conf);
		ctrljob0.setJob(job0);
		//job0的输入输出文件路径
		FileInputFormat.addInputPath(job0, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job0, path0);

		/*设置Job1*/
		Path path1 = new Path(otherArgs[1]);
		if(hdfs.exists(path1))
			hdfs.delete(path1, true);//如果MapReduce1的输出目录已存在，则删除
		Job job1 = new Job(conf, "job1-WordCounts");
		job1.setJarByClass(NaiveBayesBasedOnWord2.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapperClass(ClassWordsCountMap.class);
		job1.setMapOutputKeyClass(Text.class);//map阶段的输出的key 
		job1.setMapOutputValueClass(IntWritable.class);//map阶段的输出的value
		job1.setReducerClass(ClassWordsCountReduce.class);
		job1.setOutputKeyClass(Text.class);//reduce阶段的输出的key 
		job1.setOutputValueClass(IntWritable.class);//reduce阶段的输出的value 
		//加入控制容器 
		ControlledJob ctrljob1 = new  ControlledJob(conf);
		ctrljob1.setJob(job1);
		//job1的输入输出文件路径
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, path1);

		/*设置Job2*/
		Path path2 = new Path(otherArgs[2]);
		if(hdfs.exists(path2))
			hdfs.delete(path2, true);
		Job job2 = new Job(conf, "job2-ClassTotalWords");
		job2.setJarByClass(NaiveBayesBasedOnWord2.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(ClassTotalWordsMap.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setReducerClass(ClassTotalWordsReduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		//加入控制容器 
		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);
		//job2的输入输出文件路径
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, path2);

		/*设置Job3*/
		Path path3 = new Path(otherArgs[3]);
		if(hdfs.exists(path3))
			hdfs.delete(path3, true);
		Job job3 = new Job(conf, "job3-DiffTotalWords");
		job3.setJarByClass(NaiveBayesBasedOnWord2.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
		job3.setMapperClass(DiffTotalWordsMap.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setReducerClass(DiffTotalWordsReduce.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		//加入控制容器 
		ControlledJob ctrljob3 = new ControlledJob(conf);
		ctrljob3.setJob(job3);
		//job3的输入输出文件路径
		FileInputFormat.addInputPath(job3, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job3, path3);

		/*设置Job4*/
		Path path4 = new Path(otherArgs[5]);
		if(hdfs.exists(path4))
			hdfs.delete(path4, true);
		Job job4 = new Job(conf, "job4-Classify");
		job4.setJarByClass(NaiveBayesBasedOnWord2.class);
		job4.setInputFormatClass(SequenceFileInputFormat.class);
		job4.setOutputFormatClass(SequenceFileOutputFormat.class);
		job4.setMapperClass(ClassifyMap.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setReducerClass(ClassifyReduce.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		//加入控制容器 
		ControlledJob ctrljob4 = new ControlledJob(conf);
		ctrljob4.setJob(job4);
		//job4的输入输出文件路径
		FileInputFormat.addInputPath(job4, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job4, path4);
		
		//作业之间依赖关系
		ctrljob2.addDependingJob(ctrljob1);
		ctrljob3.addDependingJob(ctrljob1);
		ctrljob4.addDependingJob(ctrljob2);
		ctrljob4.addDependingJob(ctrljob3);
		ctrljob4.addDependingJob(ctrljob0);
		
		//主的控制容器，控制上面的子作业 		
		JobControl jobCtrl = new JobControl("NaiveBayes2");
		//添加到总的JobControl里，进行控制
		jobCtrl.addJob(ctrljob0);
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);
		jobCtrl.addJob(ctrljob3);
		jobCtrl.addJob(ctrljob4);
		
		//在线程启动
	    Thread  theController = new Thread(jobCtrl); 
	    theController.start(); 
	    while(true){
	        if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息 
	        	System.out.println(jobCtrl.getSuccessfulJobList()); 
	        	jobCtrl.stop(); 
	        	break; 
	        }
	    }  
	}

	/*
	 * MapReduce0用于得到每个类的文档总数。先验概率的分子
	 * 输入:args[0],序列化的训练集，输入格式为<（类别:docID）,word1 word2 word3...>
	 * 输出:args[6],输出key为类别,value为这一类下的文档总数.格式为<类别,Totaldocs>
	 */
	/*Map0:输入<（类别:docID）,word1 word2 word3...>
           输出<类别,1>*/
	public static class ClassTotalDocsMap extends Mapper<Text, Text, Text, IntWritable>{
		private Text newKey = new Text();
		private final IntWritable newValue = new IntWritable(1);
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			int index = key.toString().indexOf(":");
			newKey.set(key.toString().substring(0, index));//key为类别
			context.write(newKey, newValue);
		}
	}
	/*Reduce0:输入<类别,1>
              输出<类别,Totaldocs>*/
	public static class ClassTotalDocsReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
			System.out.println(key +"\t"+ result);
		}
	}


	/*
	 * MapReduce1用于处理序列化的文件，得到<（类别：word）,该单词出现总次数>
	 * 输入:args[0],序列化的训练集,key为(类名:文档名),value为文档中对应的单词。形式为<（类别：docID）,word1 tab word2 tab word3 tab word4......>
	 * 输出:args[1],key为(类名:单词),value为单词出现次数,即<(类别:word),TotalCounts>
	 */

	/*Map1:输入<（类别:DocID）,word1 word2 ...>
         输出<(类别:word),1>*/
	public static class ClassWordsCountMap extends Mapper<Text, Text, Text, IntWritable>{
		private Text newKey = new Text();
		private final IntWritable newValue = new IntWritable(1);//每一个不同的单词计数1次
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{		
			int index = key.toString().indexOf(":");//序列化训练集key=（类别:DocID），需要定位出冒号，将类别和文档名分开
			String Class = key.toString().substring(0, index);//类别
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				newKey.set(Class + ":" + itr.nextToken());//设置新键值key为（类别:word）,value为1(本类下这个单词出现了1次)
				context.write(newKey, newValue);
			}
		}		
	}
	/*Reduce1:输入<(类别:word),1>
            输出<(类别:word),单词出现的总数>*/
	public static class ClassWordsCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value:values){//汇总（类别:word）出现的总次数
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
			//System.out.println(key + "\t" + result);
		}
	}
	
	/*
	 * MapReduce2在MapReduce1计算的基础上进一步得到每个类的单词总数<类别,TotalWords>。条件概率的分母
	 * 输入:args[1],输入格式为<（类别:word）,counts>
	 * 输出:args[2],输出key为类别,value为单词总数.格式为<类别,Totalwords>
	 */
	/*Map2:输入<（类别:word）,counts>
           输出<类别,counts>*/
	public static class ClassTotalWordsMap extends Mapper<Text, IntWritable, Text, IntWritable>{
		private Text newKey = new Text();
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
			int index = key.toString().indexOf(":");
			newKey.set(key.toString().substring(0, index));//key为类别
			context.write(newKey, value);
		}
	}
    /*Reduce2:输入<类别,counts>
              输出<类别,Totalwords>*/
	public static class ClassTotalWordsReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
	    public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable value : values) {            	
	            sum += value.get();
	        }
	        result.set(sum);            
	        context.write(key, result); 
	        //System.out.println(key +"\t"+ result);
	    }
	}
	
	/*
	 * MapReduce3在MapReduce1的计算基础上得到整个训练集中不重复的单词<word,1>。条件概率的分母要用到
	 * 输入:args[1],输入格式为<(类别,word),counts>
	 * 输出:args[3],输出key为不重复单词,value为1.格式为<word,1>
	 */
	public static class DiffTotalWordsMap extends Mapper<Text, IntWritable, Text, IntWritable>{
		private Text newKey = new Text();		
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
			int index = key.toString().indexOf(":");
			newKey.set(key.toString().substring(index+1, key.toString().length()));//设置新键值key为<word>
			context.write(newKey, value);
		}
	}
	public static class DiffTotalWordsReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private final IntWritable newValue = new IntWritable(1);
	    public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {	        
	        context.write(key, newValue);
	        //System.out.println(key +"\t"+ one);
	    }
	}
	
//	/* 计算先验概率
//	 * 先验概率P(c)=类c下的单词总数/整个训练样本的单词总数
//	 * 输入:对应MapReduce2的输出,格式为<类别,totalWords>
//	 * 输出:得到HashMap<String,Double>,即<类别,概率>
//	 */
//	private static HashMap<String,Double> priorProbability = new HashMap<String,Double>();//用HashMap存不同类别的先验概率
//	public static HashMap<String,Double> GetPriorProbability() throws IOException{//计算先验概率的函数
//		Configuration conf = new Configuration();
//		String filePath = otherArgs[2];
//		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
//		Path path = new Path(filePath);
//		SequenceFile.Reader reader = null;
//		double totalWords = 0;
//		try{
//			reader = new SequenceFile.Reader(fs, path, conf);
//			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
//			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
//			long position = reader.getPosition();//设置标记点，标记文档起始位置，方便后面再回来遍历
//			while(reader.next(key,value)){
//				totalWords += value.get();//把所有类别的totalWords加起来，得到训练集总单词数(相同的单词只要出现了也重复计数)
//			}
//
//			reader.seek(position);//重置到前面定位的标记点
//			while(reader.next(key,value)){
//				priorProbability.put(key.toString(), value.get()/totalWords);//P(c)=类c下的单词总数/整个训练样本的单词总数
//				//System.out.println(key+":"+value.get()+"/"+totalWords+"\t"+value.get()/totalWords);
//			}
//		}finally{
//			IOUtils.closeStream(reader);
//		}
//		return priorProbability;
//	}

	/* 计算先验概率
	 * 先验概率P(c)=类c下的文档总数/整个训练样本的文档总数
	 * 输入:对应MapReduce0的输出,格式为<类别,Totaldocs>
	 * 输出:得到HashMap<String,Double>,即<类别,先验概率>
	 */
	private static HashMap<String,Double> priorProbability = new HashMap<String,Double>();//用HashMap存不同类别的先验概率
	public static HashMap<String,Double> GetPriorProbability() throws IOException{//计算先验概率的函数
		Configuration conf = new Configuration();
		String filePath = otherArgs[6]+"/part-r-00000";
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		SequenceFile.Reader reader = null;
		double totaldocs = 0;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();//设置标记点，标记文档起始位置，方便后面再回来遍历
			while(reader.next(key,value)){
				totaldocs += value.get();//把所有类别的totaldocs加起来，得到训练集总文档数
			}

			reader.seek(position);//重置到前面定位的标记点
			while(reader.next(key,value)){
				priorProbability.put(key.toString(), value.get()/totaldocs);//P(c)=类c下的文档数/整个训练样本的文档总数
				System.out.println(key+":"+"\t"+value.get()/totaldocs);
			}
		}finally{
			IOUtils.closeStream(reader);
		}
		return priorProbability;
	}
	
	/* 计算条件概率
	 * 条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/（类c下单词总数+训练样本中不重复特征词总数）
	 * 输入:对应MapReduce1的输出<(类别:word),counts>,MapReduce2的输出<类别,totalWords>,MapReduce3的输出<word,1>
	 * 输出:得到HashMap<String,Double>,即<（类名:单词）,条件概率>
	 */
    private static HashMap<String, Double> wordsProbability = new HashMap<String, Double>();//用来存储单词条件概率
	public static HashMap<String, Double> GetConditionProbability() throws IOException{
		Configuration conf = new Configuration();

		String ClassTotalWordsPath = otherArgs[2]+"/part-r-00000";
		String DiffTotalWordsPath = otherArgs[3]+"/part-r-00000";
		String ClasswordcountsPath = otherArgs[1]+"/part-r-00000";

		//用来计算每个类别下的单词总数（分母）
        HashMap<String, Double> ClassTotalWords = new HashMap<String, Double>();//每个类及类对应的单词总数

        FileSystem fs1 = FileSystem.get(URI.create(ClassTotalWordsPath), conf);
		Path path1 = new Path(ClassTotalWordsPath);
		SequenceFile.Reader reader1 = null;
		try{
			reader1 = new SequenceFile.Reader(fs1, path1, conf);
			Text key1 = (Text)ReflectionUtils.newInstance(reader1.getKeyClass(), conf);
			IntWritable value1 = (IntWritable)ReflectionUtils.newInstance(reader1.getValueClass(), conf);
			while(reader1.next(key1,value1)){
				ClassTotalWords.put(key1.toString(), value1.get()*1.0);
				//System.out.println(key1.toString() + "\t" + value1.get());
			}
		}finally{
			IOUtils.closeStream(reader1);
		}

		//用来计算整个训练集中不同单词数目（分母）
        double TotalDiffWords = 0.0;

		FileSystem fs2 = FileSystem.get(URI.create(DiffTotalWordsPath), conf);
		Path path2 = new Path(DiffTotalWordsPath);
		SequenceFile.Reader reader2 = null;
		try{
			reader2 = new SequenceFile.Reader(fs2, path2, conf);
			Text key2 = (Text)ReflectionUtils.newInstance(reader2.getKeyClass(), conf);
			IntWritable value2 = (IntWritable)ReflectionUtils.newInstance(reader2.getValueClass(), conf);
			while(reader2.next(key2,value2)){
				TotalDiffWords += value2.get();
			}	
			System.out.println(TotalDiffWords);
		}finally{
			IOUtils.closeStream(reader2);
		}

		//用来计算条件概率
        FileSystem fs3 = FileSystem.get(URI.create(ClasswordcountsPath), conf);
		Path path3 = new Path(ClasswordcountsPath);
		SequenceFile.Reader reader3 = null;
		try{
			reader3 = new SequenceFile.Reader(fs3, path3, conf);
			Text key3 = (Text)ReflectionUtils.newInstance(reader3.getKeyClass(), conf);//key3是（类别:word）
			IntWritable value3 = (IntWritable)ReflectionUtils.newInstance(reader3.getValueClass(), conf);
			Text newKey = new Text();
			while(reader3.next(key3,value3)){
				int index = key3.toString().indexOf(":");
				newKey.set(key3.toString().substring(0, index));//得到单词所在的类
				wordsProbability.put(key3.toString(), (value3.get()+1)/(ClassTotalWords.get(newKey.toString())+TotalDiffWords));
				//条件概率HashMap<（类别C:wordk）,类C下wordk出现的次数+1/(类C下单词总数+整个训练集总的不同单词数)>
				//System.out.println(key3.toString() + " \t" + (value3.get()+1) + "/" + (ClassTotalWords.get(newKey.toString())+ "+" +TotalDiffWords));
			}
			//对于某一个类别C中没有出现过的单词，其概率为1/(类C中单词总个数 + 整个训练集中的不同单词数)
			//遍历类，每个类别中再加一个没有出现单词的概率，其格式为<class,probably>
			for(Map.Entry<String,Double> entry:ClassTotalWords.entrySet()){
				wordsProbability.put(entry.getKey().toString(), 1.0/(ClassTotalWords.get(entry.getKey().toString()) + TotalDiffWords));
				//System.out.println(entry.getKey().toString() +"\t"+ 1.0+"/"+(ClassTotalWords.get(entry.getKey().toString()) +"+"+ TotalDiffWords));
			}
		}finally{
			IOUtils.closeStream(reader3);
		}
		return wordsProbability;
	}
	
	/*
	 * MapReduce4进行贝叶斯分类
	 * 输入:args[4],测试集的文件路径,测试数据格式<（类别:docID）,word1 word2 ...>
	 *      HashMap<String,Double> classProbability先验概率
     *      HashMap<String,Double> wordsProbability条件概率
	 * 输出:args[5],输出每一份文档经贝叶斯分类后所对应的类,格式为<docID,类别>
	 */
	public static class ClassifyMap extends Mapper<Text, Text, Text, Text>{
		public void setup(Context context)throws IOException{
			GetPriorProbability();
			GetConditionProbability();
		}
		
		private Text newKey = new Text();
		private Text newValue = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			int index = key.toString().indexOf(":");
			String docID = key.toString().substring(index+1, key.toString().length());

			//Map阶段对测试集的每一个文档，计算出它属于不同类别时的概率，即输出<docID,(类别：概率)>
			for(Map.Entry<String, Double> entry: priorProbability.entrySet()){//外层循环遍历所有类别
				String mykey = entry.getKey();//类名
				newKey.set(docID);//新的键值的key为<文档名>
                //tempvalue为对数先验概率和各单词对数条件概率的求和
				double tempvalue = Math.log(entry.getValue());//求出对数先验概率
				StringTokenizer itr = new StringTokenizer(value.toString());
				while(itr.hasMoreTokens()){//内层循环遍历一份测试文档中的所有单词	
					String tempkey = mykey + ":" + itr.nextToken();//构建临时的键（类别:word）,并在wordsProbability表中查找对应的对数条件概率
					if(wordsProbability.containsKey(tempkey)){
						//如果测试文档的单词在训练集中出现过，则直接加上之前计算的条件概率
						tempvalue += Math.log(wordsProbability.get(tempkey));
					}
					else{//如果训练集文档中没有出现国这个单词，则加上1/(类C中单词总个数 + 整个训练集中的不同单词数)这个概率
						tempvalue += Math.log(wordsProbability.get(mykey));
					}
				}
				newValue.set(mykey + ":" + tempvalue);//新的键值的value为（类别:概率）
				context.write(newKey, newValue);
				//System.out.println(newKey + "\t" +newValue);
			}
		}
	}

	public static class ClassifyReduce extends Reducer<Text, Text, Text, Text>{
		Text newValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String tempClass = null;
			double tempProbably = Double.NEGATIVE_INFINITY;//先给概率赋值为负无穷
			for(Text value:values) {
                int index = value.toString().indexOf(":");
                if (Double.parseDouble(value.toString().substring(index + 1, value.toString().length())) > tempProbably) {
                    tempClass = value.toString().substring(0, index);
                    tempProbably = Double.parseDouble(value.toString().substring(index + 1, value.toString().length()));
                }
            }
			
			newValue.set(tempClass);
			context.write(key, newValue);//<docID,类别>
			System.out.println(key + "\t" + newValue);//屏幕打印分类结果
		}
	}
}

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

 	/*贝叶斯文档分类器的多项式模型–以单词为粒度
 	条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/（类c下单词总数+训练样本中不重复的单词总数）
  	先验概率计算分别使用下述两种公示并对比效果：
  	（1）P(c)=类c下的文档总数/整个训练样本的文档总数
  	（2）P(c)=类c下的单词总数/整个训练样本的单词总数*/

public class NaiveBayes {
	static String[] otherArgs; 
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 5){
			System.err.println("error: Invalid Arguments Length!");
			System.exit(5);
		}
		
		FileSystem hdfs = FileSystem.get(conf);

		/*设置job0*/
		Path path0 = new Path(otherArgs[4]);
		if(hdfs.exists(path0))
			hdfs.delete(path0, true);//如果job0的输出目录已存在，则删除
		Job job0 = new Job(conf, "job0-DocCounts");
		job0.setJarByClass(NaiveBayes.class);
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

		/*设置job1*/
		Path path1 = new Path(otherArgs[1]);
		if(hdfs.exists(path1))
			hdfs.delete(path1, true);//如果job1的输出目录已存在，则删除
		Job job1 = new Job(conf, "job1-WordCounts");
		job1.setJarByClass(NaiveBayes.class);
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

		/*设置job2*/
		Path path2 = new Path(otherArgs[2]);
		if(hdfs.exists(path2))
			hdfs.delete(path2, true);
		Job job2 = new Job(conf, "job2-ClassTotalWords");
		job2.setJarByClass(NaiveBayes.class);
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

		/*设置job3*/
		Path path3 = new Path(otherArgs[3]);
		if(hdfs.exists(path3))
			hdfs.delete(path3, true);
		Job job3 = new Job(conf, "job3-DiffTotalWords");
		job3.setJarByClass(NaiveBayes.class);
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

		
		//作业之间依赖关系
		ctrljob2.addDependingJob(ctrljob1);
		ctrljob3.addDependingJob(ctrljob1);

		//主的控制容器，控制上面的子作业 		
		JobControl jobCtrl = new JobControl("NaiveBayes");
		//添加到总的JobControl里，进行控制
		jobCtrl.addJob(ctrljob0);
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);
		jobCtrl.addJob(ctrljob3);
		
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
	 * job0用于得到每个类的文档总数。先验概率的分子
	 * 输入:args[0],序列化的训练集，输入格式为<（类别:docID）,word1 word2 word3...>
	 * 输出:args[4],输出key为类别,value为这一类下的文档总数.格式为<类别,Totaldocs>
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
	/*Reduce0:输入<类别,{1,1,...,1}>
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
		}
	}


	/*
	 * job1用于处理序列化的训练集，得到<（类别：word）,该单词出现总次数>
	 * 输入:args[0],序列化的训练集,key为(类名:文档名),value为文档中对应的单词。形式为<（类别：docID）,word1 word2 word3......>
	 * 输出:args[1],key为(类名:单词),value为单词出现次数,即<(类别:word),该单词出现总次数>
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
	/*Reduce1:输入<(类别:word),{1,1,...,1}>
              输出<(类别:word),该单词出现总次数>*/
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
	 * job2在job1计算的基础上进一步得到每个类的单词总数<类别,TotalWords>。条件概率的分母
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
	 * job3在job1的计算基础上得到整个训练集中不重复的单词<word,1>。条件概率的分母要用到
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

}
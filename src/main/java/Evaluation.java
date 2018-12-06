import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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

/*
 * args[0]输入,测试集路径
 * args[1]输出,处理测试集得到得到正确情况下每个类有哪些文档
 * args[2]输入,经贝叶斯分类的结果
 * args[3]输出,经贝叶斯分类每个类有哪些文档
 * 屏幕输出评估值
 */


public class Evaluation {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("error: Invalid Arguments!");
			System.exit(4);
		}
		FileSystem hdfs = FileSystem.get(conf);

		Path path1 = new Path(otherArgs[1]);
		if(hdfs.exists(path1))
			hdfs.delete(path1, true);
		Job job1 = new Job(conf, "测试集处理 ");
		job1.setJarByClass(Evaluation.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapperClass(OriginalClassMap.class);
        job1.setMapOutputKeyClass(Text.class);//map阶段的输出的key
        job1.setMapOutputValueClass(Text.class);//map阶段的输出的value
		job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		//加入控制容器
		ControlledJob ctrljob1 = new  ControlledJob(conf);
		ctrljob1.setJob(job1);
		//job1的输入输出文件路径
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

		Path path2 = new Path(otherArgs[3]);
		if(hdfs.exists(path2))
			hdfs.delete(path2, true);
		Job job2 = new Job(conf, "预测结果处理");
		job2.setJarByClass(Evaluation.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(ClassifiedClassMap.class);
        job2.setMapOutputKeyClass(Text.class);//map阶段的输出的key
        job2.setMapOutputValueClass(Text.class);//map阶段的输出的value
		job2.setReducerClass(Reduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//加入控制容器
		ControlledJob ctrljob2 = new  ControlledJob(conf);
		ctrljob2.setJob(job2);
		//job1的输入输出文件路径
		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

		ctrljob2.addDependingJob(ctrljob1);

		JobControl jobCtrl = new JobControl("评估分类效果");
		//添加到总的JobControl里，进行控制
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);

		//在线程启动，记住一定要有这个
		Thread  theController = new Thread(jobCtrl);
		theController.start();
		while(true){
			if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息
				System.out.println(jobCtrl.getSuccessfulJobList());
				jobCtrl.stop();
				break;
			}
		}

		ClassifiedResultsManage(conf, otherArgs[3]+"/part-r-00000");
		GetEvaluation(conf,otherArgs[1]+"/part-r-00000");
	}

	/**
	 * 得到正确的文档分类
	 * 输入:初始数据集合,格式为<（类别:docID）,word1 word2...>
	 * 输出:原本的文档分类，即<类别,docID>
	 */
	public static class OriginalClassMap extends Mapper<Text, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newValue = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int index = key.toString().indexOf(":");
            newKey.set(key.toString().substring(0, index));
            newValue.set(key.toString().substring(index + 1, key.toString().length()));
            context.write(newKey, newValue);
            System.out.println(newKey + "\t" + newValue);
        }
    }
	
	/**
	 * 得到经贝叶斯分分类器分类后的文档分类
	 * 读取经贝叶斯分类器分类后的结果文档<docID,类别>,并将其转化为<类别,docID>的形式
	 */
	public static class ClassifiedClassMap extends Mapper<Text, Text, Text, Text>{
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{		
			context.write(value, key);
//			System.out.println(value + "\t" + key);
		}
	}
	/*输入<类别，docID>，输出<类别，docID1 docID2 docID3...>*/
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		private Text result = new Text();		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		    //生成文档列表
			String fileList = new String();
			for(Text value:values){
				fileList += value.toString() + ":";
			}
			result.set(fileList);
			context.write(key, result);
			System.out.println(key + "\t" + result);
		}
	}

	//因为后面评估分类效果时每次需要同时处理相同类别下的测试集里的文档情况和经贝叶斯分类后的文档情况，这里先用
    //HashMap处理一下经贝叶斯分类后的结果，方便后面处理。
    private static HashMap<String, String> ClassifiedClassMap = new HashMap<String, String>();
	public static HashMap <String, String> ClassifiedResultsManage(Configuration conf, String ClassifiedClassPath) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(ClassifiedClassPath), conf);
		Path path = new Path(ClassifiedClassPath);
		SequenceFile.Reader reader = null;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while (reader.next(key,value)){
				ClassifiedClassMap.put(key.toString(), value.toString());
			}
		}finally{
			IOUtils.closeStream(reader);
		}
//		System.out.println(ClassifiedClassMap);
		return ClassifiedClassMap;
    }

	/**
	 * 第一个MapReduce计算得出测试集中各个类有哪些文档,第二个MapReduce计算得出经贝叶斯分类后各个类有哪些文档
	 * 此函数作用就是统计测试集实际分类和贝叶斯预测分类两种情况下各个类公有的文档数目(即针对各个类分类正确的文档数目TP)
	 * 测试集中各个类总数目减去分类正确的数目即为原本正确但分类错误的数目(FN = OriginalCounts-TP)
	 * 贝叶斯分类得到的各个类的总数目减去分类正确的数目即为原本不属于该类但分到该类的数目(FP = ClassifiedCounts - TP)
	 */
	//Precision精度:P = TP/(TP+FP)
	//Recall精度:   R = TP/(TP+FN)
	//P和R的调和平均:F1 = 2PR/(P+R)
	//针对所有类别:  
	//Macroaveraged precision:(p1+p2+...+pN)/N
	//Microaveraged precision:对应各项相加再计算总的P、R值	
	public static void GetEvaluation(Configuration conf, String OriginalClassPath) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(OriginalClassPath), conf);
		Path path = new Path(OriginalClassPath);
		SequenceFile.Reader reader = null;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			ArrayList<String> ClassNames = new ArrayList<String>();     //依次得到分类的类名
			ArrayList<Integer> TruePositive = new ArrayList<Integer>(); //记录真实情况和经分类后，正确分类的文档数目
			ArrayList<Integer> FalseNegative = new ArrayList<Integer>();//记录属于该类但是没有分到该类的数目
			ArrayList<Integer> FalsePositive = new ArrayList<Integer>();//记录不属于该类但是被分到该类的数目
			
		
			while(reader.next(key, value)){
				//System.out.println(key1 + "\t" + key2);	//可以看到key1==key2,也就是说读入时分类前和分类后都是按相同的顺序排序的
				//=>后面可以逐条记录处理(因为每次读入的分类前后的类是相同的)
			
				ClassNames.add(key.toString());
				//ClassifiedClassNames.add(key1.toString());
				//OriginalClassNames.add(key2.toString());
				
				String[] values1 = ClassifiedClassMap.get(key.toString()).split(":");//预测分类结果
				System.out.println(values1);
				String[] values2 = value.toString().split(":");//测试集情况
											
				//System.out.println(key1.toString() + "---" + values1.length + "\t" + key2.toString() + "---" + values2.length);
				
				int TP = 0;
				for(String str1:values1){
					for(String str2:values2){
						if(str1.equals(str2)){
							TP++;
						}
					}
				}
				
				TruePositive.add(TP);
				FalsePositive.add(values1.length - TP);
				FalseNegative.add(values2.length - TP);	
				
				//System.out.println(key1.toString() + " = " + key2.toString() + ":" + values1.length + ";" + values2.length + ";" + TP + ";" + (values1.length-TP) + ";" + (values2.length-TP));
				//System.out.println(key1.toString() + "\tp=" + TP*1.0/values1.length + ";r=" + TP*1.0/values2.length);
				double pp = TP*1.0/values1.length;
				double rr = TP*1.0/values2.length;
				double ff = 2*pp*rr/(pp+rr);
				
				System.out.println(key.toString() + ":" + key.toString() + "\t" + values1.length + "\t" + values2.length +
						"\t" + TP + "\t" + (values1.length-TP) + "\t" + (values2.length-TP) + "\tp=" + pp + ";\tr=" + rr + ";\tf1=" + ff);
						
			}
			
			//Caculate MacroAverage
			double Pprecision = 0.0;
			double Rprecision = 0.0;
			double F1precision = 0.0;

			//Calculate MicroAverage
			int TotalTP = 0;
			int TotalFN = 0;
			int TotalFP = 0;			
			
			System.out.println(ClassNames.size());
			
			for(int i=0; i<ClassNames.size(); i++){			
				//MacroAverage				
				double p1 = TruePositive.get(i)*1.0/(TruePositive.get(i) + FalsePositive.get(i));
				double r1 = TruePositive.get(i)*1.0/(TruePositive.get(i) + FalseNegative.get(i));
				double f1 = 2.0*p1*r1/(p1+r1);
				//System.out.println(ClassNames.get(i)+": p1="+p1+";\tr1="+r1+"\tf1="+f1);
				Pprecision += p1;
				Rprecision += r1;
				F1precision += f1;
								
				//MicroAverage
				TotalTP += TruePositive.get(i);
				TotalFN += FalseNegative.get(i);
				TotalFP += FalsePositive.get(i);
				//System.out.println(ClassNames.get(i) + "\t" + TruePositive.get(i) + "\t" + FalseNegative.get(i) + "\t" + FalsePositive.get(i));
			}
			System.out.println("MacroAverage precision : P=" + Pprecision/ClassNames.size() +";\tR="+ Rprecision/ClassNames.size() +";\tF1="+F1precision/ClassNames.size());
//			System.out.println("MacroAverage precision : P=" + Pprecision +";\tR="+ Rprecision +";\tF1="+F1precision);
			
			double p2 = TotalTP*1.0/(TotalTP + TotalFP);
			double r2 = TotalTP*1.0/(TotalTP + TotalFN);
			double f2 = 2.0*p2*r2/(p2+r2);
			
			System.out.println("MicroAverage precision : P= " + p2 + ";\tR=" + r2 + ";\tF1=" + f2);
			
		}finally{
//			reader1.close();
			reader.close();
		}		
	}
	
	

}

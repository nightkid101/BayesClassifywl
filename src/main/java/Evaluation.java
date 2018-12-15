import java.io.IOException;
import java.net.URI;
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
		Calculate(conf,otherArgs[1]+"/part-r-00000");
	}


	  /*Job1用来处理测试集数据
	  输入:序列化的测试集,格式为<（类别:文档）,word1 word2...>
	  输出:测试集的文档分类，即<类别,（文档1,文档2,...）>*/

	  /*Map1输入:序列化的测试集,格式为<（类别:文档）,word1 word2...>
	  		输出:测试集的文档分类，即<类别,文档>*/
	public static class OriginalClassMap extends Mapper<Text, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newValue = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int index = key.toString().indexOf(":");
            newKey.set(key.toString().substring(0, index));
            newValue.set(key.toString().substring(index + 1, key.toString().length()));
            context.write(newKey, newValue);
        }
    }

	 /*Job2用来处理贝叶斯分类器的预测结果
	 输入：<docID,类别>
	 输出:贝叶斯的文档分类，即<类别,（文档1,文档2,...）>*/

	/*Map2输入:贝叶斯分类器的预测结果,格式为<docID,类别>
	  		输出:<类别,文档>*/
	public static class ClassifiedClassMap extends Mapper<Text, Text, Text, Text>{
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{		
			context.write(value, key);
		}
	}
	/*输入<类别,docID>，输出<类别，docID1:docID2:docID3:...>*/
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
		}
	}

	/*因为后面评估分类效果时每次需要同时处理相同类别下的测试集里的文档情况和经贝叶斯分类后的文档情况，这里先用
    HashMap处理一下经贝叶斯分类后的结果，方便后面处理。*/
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
		return ClassifiedClassMap;
    }


	 /*第一个MapReduce计算得出测试集中各个类有哪些文档,第二个MapReduce计算得出经贝叶斯分类后各个类有哪些文档
	 (1)TP:统计测试集实际分类和贝叶斯预测分类两种情况下各个类公有的文档数目
	 (2)FN:测试集中各个类总数目减去分类正确的数目即为原本正确但分类错误的数目(FN = OriginalCounts-TP)
	 (3)FP:贝叶斯分类得到的各个类的文档总数减去分类正确的数目(FP = ClassifiedCounts-TP)*/
	/*Precision精度:P = TP/(TP+FP)
	Recall精度:   R = TP/(TP+FN)
	P和R的调和平均:F1 = 2PR/(P+R)
	Macro-avg precision:(p1+p2+...+pN)/N
	Micro-avg precision:邻接矩阵相加求TP、FN、FP，再求Precision*/
	public static void Calculate(Configuration conf, String OriginalClassPath) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(OriginalClassPath), conf);
		Path path = new Path(OriginalClassPath);
		SequenceFile.Reader reader = null;

		int TotalClass = 0;//记录总类别数

		// 计算宏平均
		double Tprecision = 0.0;
		double Trecall = 0.0;
		double Tf1 = 0.0;

		//计算微平均
		int TotalTP = 0;
		int TotalFN = 0;
		int TotalFP = 0;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), conf);

			while(reader.next(key, value)){

				//贝叶斯预测后这个类的所有文档
				String[] values1 = ClassifiedClassMap.get(key.toString()).split(":");
				//测试集中对应类的所有文档
				String[] values2 = value.toString().split(":");

				//计算这个类下的TP值
				int TP = 0;
				for(String str1:values1){
					for(String str2:values2){
						if(str1.equals(str2)){
							TP++;
						}
					}
				}

				double precision = TP*1.0/values1.length;
				double recall = TP*1.0/values2.length;
				double f1 = 2*precision*recall/(precision+recall);

				System.out.println("类别：" + key.toString() + "\tTP：" + TP + "\tFP:" + (values1.length - TP) + "\tFN:" + (values2.length - TP) + "\tPrecision:" + precision + "\tRecall:" + recall + "\tF1:" + f1);

				TotalClass ++;
				Tprecision += precision;
				Trecall += recall;
				Tf1 += f1;

				TotalTP += TP;
				TotalFN += values2.length - TP;
				TotalFP += values1.length - TP;
			}

			//MacroAverage
			double Macrop = Tprecision/TotalClass;
			double Macror = Trecall/TotalClass;
			double Macrof1 = Tf1/TotalClass;
								
			//MicroAverage
			double Microp = TotalTP*1.0/(TotalTP+TotalFP);
			double Micror = TotalTP*1.0/(TotalTP+TotalFN);
			double Microf1 = 2.0*Microp*Micror/(Microp+Micror);

			System.out.println("MacroAverage Precision:" + Macrop + "\tRecall:" + Macror + "\tF1:" + Macrof1);

			System.out.println("MicroAverage precision:" + Microp + "\tRecall:" + Micror + "\tF1:" + Microf1);
		}finally{
			reader.close();
		}		
	}
}

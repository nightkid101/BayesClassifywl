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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Prediction2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5){
            System.err.println("error: Invalid Arguments Length!");
            System.exit(5);
        }

        GetPriorProbability(otherArgs[1]+"/part-r-00000");
        GetConditionProbability(otherArgs[0]+"/part-r-00000",otherArgs[1]+"/part-r-00000",otherArgs[2]+"/part-r-00000");

        FileSystem hdfs = FileSystem.get(conf);

        /*设置Job4*/
        Path path = new Path(otherArgs[4]);
        if(hdfs.exists(path))
            hdfs.delete(path, true);
        Job job = new Job(conf, "job4-Classify");
        job.setJarByClass(Prediction2.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(ClassifyMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ClassifyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job4的输入输出文件路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
        FileOutputFormat.setOutputPath(job, path);

        /* 启动Job */
        System.exit(job.waitForCompletion(true)?0:1);
    }

    /* 计算先验概率
	 * 先验概率P(c)=类c下的单词总数/整个训练样本的单词总数
	 * 输入:对应MapReduce2的输出,格式为<类别,totalWords>
	 * 输出:得到HashMap<String,Double>,即<类别,概率>
	 */
	private static HashMap<String,Double> priorProbability = new HashMap<String,Double>();//用HashMap存不同类别的先验概率
	public static HashMap<String,Double> GetPriorProbability(String filePath) throws IOException{//计算先验概率的函数
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		SequenceFile.Reader reader = null;
		double totalWords = 0;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();//设置标记点，标记文档起始位置，方便后面再回来遍历
			while(reader.next(key,value)){
				totalWords += value.get();//把所有类别的totalWords加起来，得到训练集总单词数(相同的单词只要出现了也重复计数)
			}

			reader.seek(position);//重置到前面定位的标记点
			while(reader.next(key,value)){
				priorProbability.put(key.toString(), value.get()/totalWords);//P(c)=类c下的单词总数/整个训练样本的单词总数
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
    public static HashMap<String, Double> GetConditionProbability(String ClasswordcountsPath,String ClassTotalWordsPath,String DiffTotalWordsPath) throws IOException{
        Configuration conf = new Configuration();

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
    public static class ClassifyMap extends Mapper<Text, Text, Text, Text> {
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
            }
        }
    }

    public static class ClassifyReduce extends Reducer<Text, Text, Text, Text> {
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
        }
    }
}

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//FileSerializer:
//将训练集和测试集中的文件序列化为<（类别：docID）,word1 word2 word3 word4...>的形式
public class FileSerializer {
//*args[0]输入文件夹路径;hdfs:///Data/training(hdfs:///Data/test)
//*args[1]输出文件夹路径;hdfs://seqtraining(hdfs://seqtest)
//*将一个文件夹（训练集/测试集）下的所有文件序列化；
    public static void main(String[] args) throws  IOException{
        String inputuri = args[0];//hdfs上训练集(/测试集)输入路径
        String outputuri = args[1];//hdfs上序列化后的训练集(/测试集)输出路径
        Configuration conf = new Configuration();
        FileSystem inputfs = FileSystem.get(URI.create(inputuri),conf);
        FileSystem outputfs = FileSystem.get(URI.create(outputuri), conf);
        FileStatus[] status = inputfs.listStatus(new Path(inputuri));//status是训练集（测试集）下不同类型文件夹的数组

        Text key = new Text();
        Text value = new Text();

        SequenceFile.Writer writer = null;
        try{
            writer = SequenceFile.createWriter(outputfs, conf, new Path(outputuri), key.getClass(), value.getClass());
            for(FileStatus dir:status){//dir是训练集（测试集）下一层某一类的文件夹
                Path path = dir.getPath();
                FileSystem characterfs = FileSystem.get(URI.create(inputuri+'/'+path.getName()), conf);
                FileStatus[] files = characterfs.listStatus(path);//files是某一类下所有文件的数组

                for(FileStatus file:files) {
                    key.set(dir.getPath().getName() + ':' + file.getPath().getName());//key是（类名:docID）
                    value.set(file2string(file));
                    writer.append(key,value);
                }
            }
        }
        finally {
            IOUtils.closeStream(writer);
        }
    }

    private static String file2string(FileStatus file) throws IOException{
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream hdfsInStream = fs.open(file.getPath());
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
        String line = null;
        String result = null;
        while((line = br.readLine()) != null){
            result += line + "  ";//单词间用tab分开
        }
        br.close();
        return result;
    }

}

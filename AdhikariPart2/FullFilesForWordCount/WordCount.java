import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();  // based on kpshadoop.blogspot.com/2014/06/word-co-occurrence-problem.html
      String[] parsed_words = line.split("\\W+");
      String[] stop_words = new String[]{"would","said","amp","http", "please","go","must",

		"ourselves","you","new","get","tell","put","try","also","going","say","could","many","de",

		"la","et","en", "your","yours","yourself","yourselves","he","him","his","himself","herself",

		"itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those",

		"am","is","are","was","were","be","been","she","her","hers","being","have","has","had","having",

		"we","our","ours","if","or","because","from","up","down","where","why","how",

		"an","the","and","but","as","until","while","of","at","by","for","with","about","against",

		"between","into","through","during","before","after","above","below","to","in","out","on",

		"off","over","under","again","further","then","once","here","there","when","all","any",

		"both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than",

		"too","very","s","t","can","will","just","don","should","now","it","its","nytimes","com","archives","feedback",

		"home","page","reports","subscribe","https","archive_feedback",

	  "mr","mrs","i","me","my","myself","do","does","did","doing","a"};
      ArrayList<String>  stopList = new ArrayList<String> (Arrays.asList(stop_words));
      for(int i=0; i < parsed_words.length;i++){
          parsed_words[i] = parsed_words[i].trim().toLowerCase();
          if(parsed_words[i].contains("\\>")  || parsed_words[i].contains("\\<") || parsed_words[i].matches(".*\\d+.*") ||
          parsed_words[i].equalsIgnoreCase("co") || parsed_words[i].equalsIgnoreCase("u") || parsed_words[i].equalsIgnoreCase("RT")
          || parsed_words[i].equalsIgnoreCase("") || parsed_words[i].length() == 1 || parsed_words[i].contains("\\_") ||
          parsed_words[i].contains("\\t+") || stopList.contains(parsed_words[i])){
            continue;    
          }
          word.set(parsed_words[i]);
          context.write(word,one);
      }
    }
  }

  public static class IntSumReducer
  extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
    Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

// Source - https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
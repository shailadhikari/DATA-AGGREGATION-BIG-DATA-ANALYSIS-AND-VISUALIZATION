import java.io.IOException;

import java.util.*;



import java.util.StringTokenizer;

import java.util.Set;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.io.WritableComparable;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.MapWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import java.io.DataInput;

import java.io.DataOutput;

/**
* Author - Shailesh
*/

public class WordCooccurrence {



	/**

	* POJO class for creating paris of words

	*/

	public static class Pair implements Writable, WritableComparable<Pair>{



    private Text first;

    private Text second;



    public Pair(Text first, Text second) {

        this.first = new Text(first);

        this.second = new Text(second);

    }



    public Pair() {

        this.first = new Text();

        this.second = new Text();

    }



		public void setFirst(String first){

        this.first.set(first);

    }

    public void setSecond(String second){

        this.second.set(second);

    }



		public Text getFirst() {

        return first;

    }



    public Text getSecond() {

        return second;

    }



		@Override

	 public void write(DataOutput out) throws IOException {

			 first.write(out);

			 second.write(out);

	 }



	 @Override

    public void readFields(DataInput in) throws IOException {

        first.readFields(in);

        second.readFields(in);

    }



		@Override

    public String toString() {

        return "{"+first+","+second+"}";

    }



    @Override

    public boolean equals(Object o) {

        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;



        Pair pair = (Pair) o;



        if (second != null ? !second.equals(pair.second) : pair.second != null) return false;

        if (first != null ? !first.equals(pair.first) : pair.first != null) return false;



        return true;

    }



    @Override

    public int hashCode() {

        int result = first != null ? first.hashCode() : 0;

        result = 163 * result + (second != null ? second.hashCode() : 0);

        return result;

    }



		@Override

    public int compareTo(Pair other) {

        int result = this.first.compareTo(other.getFirst());

        if(result != 0){

            return result;

        }

        if(this.second.toString().equals("*")){

            return -1;

        }else if(other.getSecond().toString().equals("*")){

            return 1;

        }

        return this.second.compareTo(other.getSecond());

    }



	}



	// Mapper for pair occurrence

	public static class PairOccurMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {

	    private Pair pair = new Pair();

	    private IntWritable intW = new IntWritable(1);



	    @Override



	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



        int nbors = context.getConfiguration().getInt("neighbors", 2);

        String[] words = value.toString().split("\\W+");

				if (words.length > 1) {

          for (int x = 0; x < words.length; x++) {

						if(!isValid(words[x]=words[x].trim().toLowerCase()))

							continue;

            pair.setFirst(words[x]);

            int start = (x - nbors < 0) ? 0 : x - nbors;

            int end = (x + nbors >= words.length) ? words.length - 1 : x + nbors;

            for (int y = x+1; y <= end; y++) {

                if (y == x || !isValid(words[y]=words[y].trim().toLowerCase()))

									continue;

                pair.setSecond(words[y]);

                context.write(pair, intW);

            }

         }

      }

	 }





		static final String[] stop_words = new String[]{"would","said","amp","please","go","must",

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

		static final ArrayList<String>  stop_words_list = new ArrayList<String> (Arrays.asList(stop_words));



		static boolean isValid(String word){



			if(word.contains("\\>")  || word.contains("\\<") || word.matches(".\\d+.") ||

			word.equalsIgnoreCase("co") || word.equalsIgnoreCase("u") || word.equalsIgnoreCase("RT")

			|| word.equalsIgnoreCase("") || word.length() == 1 || word.contains("\\_") ||

			word.contains("\\t+") || stop_words_list.contains(word))

				return false;



			return true;

		}



	}







  // Mapper for pair occurrence count

	public static class PairCountReducer extends Reducer<Pair,IntWritable,Pair,IntWritable> {



			private Map<Pair, IntWritable> countMap = new HashMap<Pair, IntWritable>();



	    @Override

	    protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

	        int count = 0;

					IntWritable totalCount = new IntWritable();

	        for (IntWritable value : values)

	             count += value.get();

	        totalCount.set(count);

					//System.out.println("Key : "+key.toString()+", value : "+totalCount);

					countMap.put(new Pair(key.getFirst(), key.getSecond()), totalCount);

	    }



			protected void cleanup(Context context) throws IOException, InterruptedException {

				List<Map.Entry<Pair, IntWritable>> entries = new LinkedList<Map.Entry<Pair, IntWritable>>(countMap.entrySet());



				Collections.sort(entries, new Comparator<Map.Entry<Pair, IntWritable>>() {

            @Override

            public int compare(Map.Entry<Pair, IntWritable> o1, Map.Entry<Pair, IntWritable> o2) {

                return o2.getValue().compareTo(o1.getValue());

            }

        });



        Map<Pair, IntWritable> sortedMap = new LinkedHashMap<Pair, IntWritable>();

        for (Map.Entry<Pair, IntWritable> entry : entries)

            sortedMap.put(entry.getKey(), entry.getValue());



        int counter = 0;

        for (Pair key : sortedMap.keySet()) {

          //if (counter++ == 20)

              //break;

      		context.write(key, sortedMap.get(key));

      	}

			}

		}



	public static void main(String[] args) throws Exception {

	    Configuration conf = new Configuration();

	    Job job = Job.getInstance(conf, "word cooccurrence");

	    job.setJarByClass(WordCooccurrence.class);

	    job.setMapperClass(PairOccurMapper.class);

	    job.setReducerClass(PairCountReducer.class);

	    job.setOutputKeyClass(Pair.class);

	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));

	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	  }



}

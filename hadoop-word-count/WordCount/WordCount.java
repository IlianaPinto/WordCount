import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		static enum Counters {
			INPUT_WORDS
		}

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();

		private long numRecords = 0;
		private String inputFile;

		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
			inputFile = job.get("map.input.file");

			if (job.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(job);
				} catch (IOException ioe) {
					System.err.println(
							"Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
				}
				for (Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}
		}

		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : "
						+ StringUtils.stringifyException(ioe));
			}
		}

		public String min(String a, String b) {

			return a.compareTo(b) > 0 ? b : a;
		}

		public String max(String a, String b) {

			return a.compareTo(b) > 0 ? a : b;
		}

		public String middleTerm(String a, String b, String c) {
			return a.compareTo(b) > 0 ? (a.compareTo(c) < 0 ? a : b.compareTo(c) > 0 ? b : c)
					: (a.compareTo(c) > 0 ? a : (b.compareTo(c) < 0 ? b : c));
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			line = line.toLowerCase();
			String[] wordPatterns = new String[]{"a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along",
                "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything",
                "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand",
                "behind", "being", "below", "beside", "besides", "between", "beyond", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co",
                "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
                "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill",
                "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go",
                "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself",
                "his", "how", "however", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter",
                "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly",
                "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone",
                "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our",
                "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming",
                "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something",
                "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence",
                "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three",
                "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up",
                "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby",
                "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within",
                "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"};

			for (int i = 33; i < 256; i++) {
				if ((i > 64 && i < 91) || (i > 96 && i < 123)) {
					char newChar = (char) i;
					String newPattern = " " + newChar + " ";

					line = line.replaceAll(newPattern, " ");
				} else if (i > 47 && i < 58) {
					char newChar = (char) i;
					String newPattern = "" + newChar;

					line = line.replaceAll(newPattern, "");
				} else {
					char newChar = (char) i;
					String newPattern = "\\" + newChar;

					line = line.replaceAll(newPattern, "");
				}
			}

			for (int i = 0; i < wordPatterns.length; i++) {
				line = line.replaceAll(" " + wordPatterns[i] + " ", " ");
			}

			String[] lineWordsTemp = line.split("\\s");
			int eliminado = 0;

			for (int i = 0; i < wordPatterns.length; i++) {
				for (int j = 0; j < lineWordsTemp.length; j++) {
					if (wordPatterns[i].equals(lineWordsTemp[j])) {
						lineWordsTemp[j] = " ";
						eliminado++;
					}

					for (int k = 33; k < 256; k++) {
						if ((k > 64 && k < 91) || (k > 96 && k < 123)) {
							char newChar = (char) k;
							String newPattern = "" + newChar;

							if (newPattern.equals(lineWordsTemp[j])) {
								lineWordsTemp[j] = " ";
								eliminado++;
							}

						} else if (k > 47 && k < 58) {
							char newChar = (char) k;
							String newPattern = "" + newChar;

							if (newPattern.equals(lineWordsTemp[j])) {
								lineWordsTemp[j] = " ";
								eliminado++;
							}

						} else {
							char newChar = (char) k;
							String newPattern = "\\" + newChar;

							if (newPattern.equals(lineWordsTemp[j])) {
								lineWordsTemp[j] = " ";
								eliminado++;
							}

						}
					}
				}
			}

			String[] lineWords = new String[lineWordsTemp.length - eliminado];
			int indexLineWords = 0;

			for (int i = 0; i < lineWordsTemp.length; i++) {
				if (!lineWordsTemp[i].equals(" ")) {
					lineWords[indexLineWords] = lineWordsTemp[i];
					indexLineWords++;
				}
			}

			String Primera_Palabra = "";
			String Segunda_Palabra = "";
			String Tercera_Palabra = "";
			for (int i = 0; i < lineWords.length; i++) {
				
				 Primera_Palabra = lineWords[i];
				 if (!Primera_Palabra.equals("")) {
				 	words.add(Primera_Palabra);
				 }
				 word.set(Primera_Palabra);
				 output.collect(word, one);
				 reporter.incrCounter(Counters.INPUT_WORDS, 1);

				for (int j = i + 1; j < lineWords.length; j++) {
					Segunda_Palabra = lineWords[j];
					if (Primera_Palabra.equals(Segunda_Palabra)) {
						continue;
					}

					String keyWord = Primera_Palabra + " " + Segunda_Palabra;
					String inverseKey = Segunda_Palabra + " " + Primera_Palabra;
					if (Primera_Palabra.compareTo(Segunda_Palabra) > 0) {
						keyWord = inverseKey;
					}

					/*word.set(keyWord);
					output.collect(word, one);
					reporter.incrCounter(Counters.INPUT_WORDS, 1);*/

					for (int k = j + 1; k < lineWords.length; k++) {						
						Tercera_Palabra = lineWords[k];

						if (Primera_Palabra.equals(Tercera_Palabra) || Segunda_Palabra.equals(Tercera_Palabra)
								|| Primera_Palabra.equals(Segunda_Palabra)) {
							continue;
						}

						/*String primera = min(min(Primera_Palabra, Segunda_Palabra), Tercera_Palabra);
						String ultima = max(max(Primera_Palabra, Segunda_Palabra), Tercera_Palabra);
						String segunda = segundaTerm(Primera_Palabra, Segunda_Palabra, Tercera_Palabra);*/
						/*keyWord = primera + " " + segunda + " " + ultima;
							if (primera.equals("") && segunda.equals("")||primera.equals("")&& ultima.equals("") || segunda.equals("")&& ultima.equals("")) {
								keyWord = "";
							}
							if (!keyWord.equals("")) {
								words.add(keyWord);
							}
							word.set(keyWord);

						output.collect(word, one);
						reporter.incrCounter(Counters.INPUT_WORDS, 1);*/
					}
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			boolean writeKey = true;

			for (int j = 0; j < mostRepeatedWords.length; j++) {
				if (mostRepeatedWords[j].equals(key)) {
					writeKey = false;
					break;
				}
			}

			if (writeKey) {

				for (int i = mostRepeatedValues.length - 1; i >= 0; i--) {
					if (sum > mostRepeatedValues[i] && writeKey) {
						if (i < mostRepeatedValues.length - 1) {
							mostRepeatedValues[i + 1] = mostRepeatedValues[i];
							mostRepeatedWords[i + 1] = mostRepeatedWords[i];
							mostRepeatedValues[i] = 0;
							mostRepeatedWords[i] = "";
						}
						mostRepeatedValues[i] = sum;
						mostRepeatedWords[i] = key.toString();
					}
				}

			}

			output.collect(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
				conf.setBoolean("wordcount.skip.patterns", true);
			} else {
				other_args.add(args[i]);
			}
		}

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.out.println("======================================================");		
		int count = 0;
		String word = "";
		String comb = "";
		for (int i = 0; i < words.size(); i++) {
			word = words.get(i);
			count++;
			for (int k = 0; k < words.size(); k++) {
				if (word.equals(words.get(k))) {
					count++;
				}
			}
			comb = "" + count + "," + word;
			counting.add(comb);
			count = 0;
		}

		for (int i = 0; i < 20; i++) {
			top20[i] = TOP();
		}
		for (int i = 0; i < 20; i++) {
			System.out.println("Top " + (i+1) + ": " + top20[i]);
		}
		System.out.println("======================================================");
		System.exit(res);
	}
	public static String TOP() {
		String value_ = "";
		String value_temp = "0,0";
		String send = "";
		int pos = 0;
		String[] output;
		String[] output1;
		for (int i = 0; i <= counting.size(); i++) {
			if (i + 1 < counting.size()) {
				output = counting.get(i).split(",");
				output1 = counting.get(i + 1).split(",");
			} else {
				break;
			}
			if (Integer.parseInt(output[0]) > Integer.parseInt(output1[0])) {
				value_ = counting.get(i);
				if (Integer.parseInt(value_temp.split(",")[0]) < Integer.parseInt(value_.split(",")[0])) {
					value_temp = counting.get(i);
					pos = i;
				}
			} else {
				value_ = counting.get(i + 1);
				if (Integer.parseInt(value_temp.split(",")[0]) < Integer.parseInt(value_.split(",")[0])) {
					value_temp = counting.get(i);
					pos = i + 1;
				}
			}
		}
		while (counting.remove(value_temp)) {
		}
		send = value_temp;
		value_temp = "0,0";
		return send;
	}
	static String[] mostRepeatedWords = { "", "", "", "", "" };
	static int[] mostRepeatedValues = { 0, 0, 0, 0, 0 };
	static String[] top20 = { "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "" };
	static int[] itop10 = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	static int checking = 0;
	static List<String> words = new ArrayList();
	static ArrayList<String> counting = new ArrayList();
	static ArrayList<String> work = new ArrayList();
	static int flag = 0;
}

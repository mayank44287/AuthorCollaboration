package wordcount
import java.io.IOException
import java.util._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

object WordCount {
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter) {
      val line: String = value.toString
      line.split(" ").foreach { token =>
        word.set(token)
        output.collect(word, one)
      }
    }
  }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    @throws[IOException]
    def reduce(key: Text, values: Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter) {
      import scala.collection.JavaConversions._
      val sum = values.toList.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))
    }
  }
  def main(args: Array[String]) {
    val conf1: JobConf = new JobConf(this.getClass)
    conf1.setJobName("WordCountScala")
    conf1.setOutputKeyClass(classOf[Text])
    conf1.setOutputValueClass(classOf[IntWritable])
    conf1.setMapperClass(classOf[Map])
    conf1.setCombinerClass(classOf[Reduce])
    conf1.setReducerClass(classOf[Reduce])
    conf1.setInputFormat(classOf[TextInputFormat])
    conf1.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf1, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf1, new Path(args(1)))
    JobClient.runJob(conf1)
  }
}

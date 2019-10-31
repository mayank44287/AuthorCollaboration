package mayankraj.hw2.mapreduce


import mayankraj.hw2.mapreduce.XmlInputFormat.MultiTagXmlRecordReader


import java.io.IOException
import java.nio.charset.StandardCharsets

import com.google.common.io.Closeables
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
 * Splits XML files by the specified start and end tags to send as input to Mappers.
 *
 * Produces key-value pairs of type [LongWritable, Text]. Multiple start and end tags can be specified. XML is split
 * according to whichever tag is matched first while scanning the file. To specify the start and end tags, set
 * {@link MultiTagXmlInput#START_TAG_KEY} and {@link MultiTagXmlInput#END_TAG_KEY} keys while configuring your
 * map-reduce job. Multiple tags should be comma-separated without spaces.
 *
 * This implementation is based on Apache Mahout's XMLInputFormat.
 */
class XmlInputFormat extends TextInputFormat with LazyLogging {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    try {
      new MultiTagXmlRecordReader(split.asInstanceOf[FileSplit], context.getConfiguration)
    }
    catch {
      case ioe: IOException =>
        logger.warn("Error while creating XmlRecordReader", ioe)
        null
    }
  }
}

/**
 * Splits XML files by the specified start and end tags to send as input to Mappers.
 *
 * Produces key-value pairs of type [LongWritable, Text]. Multiple start and end tags can be specified. XML is split
 * according to whichever tag is matched first while scanning the file. To specify the start and end tags, set
 * {@link MultiTagXmlInput#START_TAG_KEY} and {@link MultiTagXmlInput#END_TAG_KEY} keys while configuring your
 * map-reduce job. Multiple tags should be comma-separated without spaces.
 *
 * This implementation is based on Apache Mahout's XMLInputFormat.
 */
object XmlInputFormat {

  //
  val start_tag_key = "multitagxmlinput.start"
  //
  val end_tag_key = "multitagxmlinput.end"
  //
  @throws[IOException]
  class MultiTagXmlRecordReader(split: FileSplit, conf: Configuration)
    extends RecordReader[LongWritable, Text] {

    private val startTags = conf.getStrings(start_tag_key).map(_.getBytes(StandardCharsets.UTF_8))
    private val endTags = conf.getStrings(end_tag_key).map(_.getBytes(StandardCharsets.UTF_8))

    private val startEndTagMapping = startTags.zip(endTags).toMap

    private val start = split.getStart
    private val end = start + split.getLength

    private val fin = split.getPath.getFileSystem(conf).open(split.getPath)
    fin.seek(start)

    //
    private val dataBuffer = new DataOutputBuffer()

    //
    private val currKey = new LongWritable()
    private val currValue = new Text()


    private var tagMatched = Array[Byte]()

    override def nextKeyValue(): Boolean = {
      //readNext(currentKey, currentValue)
      if (fin.getPos < end && readUntilMatch(startTags, false)) {
        try {
          // Store the matched tag in the dataBuffer (Our output will start with the start tag)
          dataBuffer.write(tagMatched)

          // Keep reading until the corresponding end tag is matched
          if (readUntilMatch(Array(startEndTagMapping(tagMatched)), true)) {

            // Emit the key and value once the end tag is matched
            currKey.set(fin.getPos)
            currValue.set(dataBuffer.getData, 0, dataBuffer.getLength)
            return true
          }
        }
        finally {
          // Reset the buffer so that we start fresh next time
          dataBuffer.reset()
        }
      }
      false

    }




   //
    private def readUntilMatch(tags: Array[Array[Byte]], bool: Boolean): Boolean = {
      // Trackers for the bytes that have been currently matched for each tag. Initialized to 0 at the beginning.
      val matchedCount: Array[Int] = tags.indices.map(_ => 0).toArray

      while (true) {
        // Read a byte from the input stream
        val currByte = fin.read()

        // Return if end of file is reached
        if (currByte == -1) {
          return false
        }

        // If we are looking for the end tag, buffer the file contents until we find it.
        if (bool) {
          dataBuffer.write(currByte)
        }

        // Check if we are matching any of the tags
        tags.indices.foreach { tagIndex =>
          // The current tag which we are testing for a match
          val tag = tags(tagIndex)

          if (currByte == tag(matchedCount(tagIndex))) {
            matchedCount(tagIndex) += 1

            // If the counter for this tag reaches the length of the tag, we have found a match
            if (matchedCount(tagIndex) >= tag.length) {
              tagMatched = tag
              return true
            }
          }
          else {
            // Reset the counter for this tag if the current byte doesn't match with the byte of the current tag being
            // tested
            matchedCount(tagIndex) = 0
          }
        }
        // Check if we've passed the stop point
        if (!bool && matchedCount.forall(_ == 0) && fin.getPos >= end) {
          return false
        }
      }
      false
    }

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

    override def getCurrentKey: LongWritable = {
      new LongWritable(currKey.get())
    }

    override def getCurrentValue: Text = {
      new Text(currValue)
    }

    override def getProgress: Float = (fin.getPos - start) / (end - start).toFloat

    override def close(): Unit = Closeables.close(fin, true)
  }

}
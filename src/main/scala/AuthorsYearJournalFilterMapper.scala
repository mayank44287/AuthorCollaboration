//package mayankraj.hw2.AuthorsYearJournalFilterMapper
package mappers

import mayankraj.hw2.mapreduce
import javax.xml.parsers.DocumentBuilderFactory
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io.{FloatWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.XML


/**
 *
 * this mapper performs similar to aoauthors bin mapper but it stratifies the data based on given year and venues(journals/publications)
 * in the configuration files
 */

class AuthorsYearJournalFilterMapper extends Mapper[LongWritable,Text,Text,IntWritable] with LazyLogging {
  val configSettings =  ConfigFactory.load()
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  val one = new IntWritable(1)

  val authorkey = new Text()
  val settings = ConfigFactory.load()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    //
    logger.trace(s"Mapper invoked: map(key: $key, value: ${value.toString}")
    val xmlString = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>"

    val publElement = XML.withSAXParser(xmlParser).loadString(xmlString)

    val authorLookupTag = publElement.child.head.label match {
      case "book" | "proceedings" => "editor"
      case _ => "author"

    }
    val year = (publElement \\ "year").map(year =>year.text).toList
    val journal =( publElement \\ "journal").map(journal =>journal.text)

    val p = year filter settings.getStringList("YEAR").contains
    val q = journal filter settings.getStringList("JOURNAL").contains
    if ((p.nonEmpty ) || (q.nonEmpty)){
      val authors = (publElement \\ authorLookupTag).map{author=>author.text}.toList
      var binSize = 0
      if (authors.nonEmpty){
        generateAuthorBins(authors).foreach{_ =>
          binSize +=1
        }

        authorkey.set(new Text(binSize.toString))
        logger.info(s"Mapper emit: <$authorkey, ${one.get()}>")
        context.write(authorkey, one)
    }

    }

  }

  def generateAuthorBins(author: Seq[String]): Iterator[String]  = {
    author.size match {
      // If the list of faculty is empty, return an empty list
      case 0 => Iterator()
      // If there is only one faculty, return only the faculty
      //case 1 => author.iterator
      // If there are more than one faculty, return all the individual faculty names, along with all possible
      // combinations of the faculty, with each pair joined by the `edgeIndicator`
      case _ => author.iterator
    }
  }

}

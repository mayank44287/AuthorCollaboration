//package mayankraj.hw2.CoAuthorNumberMapper
package mappers
import mayankraj.hw2.mapreduce
import javax.xml.parsers.DocumentBuilderFactory
import java.io.ByteArrayInputStream
import java.io.InputStream
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import scala.xml.{Elem, XML}


/**
 * This mapper class implements for the job which asks to create a bins of number of coauthors against the number of
 *publications. It iterates over each record, counts the number of authors in the iterated record and emits the (key,value) of
 * (number of authors,1)
 */

class CoAuthorNumberMapper extends Mapper[LongWritable,Text,Text,IntWritable] with LazyLogging {
  val configSettings =  ConfigFactory.load()
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  val one = new IntWritable(1)

  val authorAndCoauthorKey = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.trace(s"Mapper invoked: map(key: $key, value: ${value.toString}")

    val xmlString = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>"

    val publElement = XML.withSAXParser(xmlParser).loadString(xmlString)

    val authorLookupTag = publElement.child.head.label match {
      case "book" | "proceedings" => "editor"
      case _ => "author"
    }
    val authors = (publElement \\ authorLookupTag).map(author=>author.text).toList
    var binSize = 0
    if (authors.nonEmpty){
      generateAuthorBins(authors).foreach{_ =>
        binSize +=1
      }
      generateAuthorBins(authors).foreach { authorName =>
        authorAndCoauthorKey.set(new Text((authorName.toString + "," + binSize.toString)))
        logger.info(s"Mapper emit: <$authorAndCoauthorKey, ${one.get()}>")
        context.write(authorAndCoauthorKey, one)
      }
    }


  }

  def generateAuthorBins(author: Seq[String]): Iterator[String]  = {
    logger.trace(s"Mapper invoked: map(key: $author")
    author.size match {

      case 0 => Iterator()

      case _ => author.iterator
    }
  }

}

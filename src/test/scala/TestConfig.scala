import com.typesafe.config.ConfigFactory
import org.apache.commons.configuration.ConfigurationFactory
import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec


class TestConfig extends FlatSpec {

  val config = new Configuration
  val conf = ConfigFactory.load()

  "Loading the tags" should "return list if tags" in{
    val ans = conf.getString("START_TAGS")
    assert(ans.split(",").length == 9)

    val ans2 = conf.getString("END_TAGS")
    assert(ans2.split(",").length==9)
  }

  "Config loader" should "load the year values" in{
    val ans = conf.getStringList("YEAR")
    assert(ans.size()==2)
  }

  "Config loader" should "load the journal values" in{
    val ans = conf.getStringList("JOURNAL")
    assert(ans.size()==2)
  }

  "Config loader" should "load the MONTH values" in{
    val ans = conf.getStringList("MONTH")
    assert(ans.size()==3)
  }

  "Load the .dtd file for the XML" should "load the dtd's path as URI" in {
    val file = getClass.getClassLoader.getResource("dblp.dtd").toURI
    assert(file.toString.length > 0)

  }




}

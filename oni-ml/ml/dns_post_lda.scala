val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import breeze.stats.DescriptiveStats._
import breeze.linalg._
import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

def isNumeric(input: String): Boolean = {
    if (input == "") {false}
    else {input.forall(x => (x.isDigit || x == '.' || x =='E' || x == 'e')) && input.count(_ == '.')<2 && (input.count(_ == 'E') + input.count(_ == 'e')) <2}
}
def toDouble(s: String, default:Double=Double.NaN) = {if (isNumeric(s)) s.toDouble else default}
//def isHeader(line: Array[String], header: Array[String]) = line.deep == header.deep

def removeHeader(input: org.apache.spark.rdd.RDD[String]) = {
    val header = input.first
    val output = input.filter(line => !(line == header))
    output
}


def get_column_names(input: org.apache.spark.rdd.RDD[String], sep : Char = ',') : scala.collection.mutable.Map[String, Int] = {
    val columns = scala.collection.mutable.Map[String, Int]()
    val header = input.first.split(sep).zipWithIndex
    columns("day") = 0
    header.foreach(tuple => columns(tuple._1) = tuple._2+1)
    columns
}

def print_columns(columns : scala.collection.mutable.Map[String, Int]) = {
    val arr = columns.toArray
    arr.sortBy(_._2).foreach(println)
}

/**
def bin_column(row: Array[String], column: String, cuts: Array[Double]) = {
    var bin = 0
    for (cut <- cuts){
        if (row(col(column)).toDouble > cut) { bin = bin+1 }
    }
    col(column + "_bin") = row.length + 1   //update the column index automatically
    row :+ bin.toString
}


def print(input: org.apache.spark.rdd.RDD[String]) = input.take(10).foreach(println)


// Load and parse the data

class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array:Array[String], key:String):String = array(index(key))
}





//----------Inputs-------------
val file = System.getenv("DPATH")
val topic_mix_file = System.getenv("HPATH") + "/doc_results.csv"
val pword_file = System.getenv("HPATH")+"/word_results.csv"
val scored_output_file = System.getenv("HPATH") + "/scored"
val threshold : Double = system.getenv("TOL").toDouble


//val outputfile = "/user/history/hiveflow/netflow/dns_test_word_count.csv"
val quant = Array(0, 0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
val domain_freq_cuts = Array(0.0, 0.0016221675888432989, 0.003437437437664978, 0.003504835695562082, 0.0050467239654149575, 0.013090365707594635, 0.016089127244084477, 0.10621307500188512, 0.1597068462448228)
val frame_length_cuts = Array(0.0, 97.0, 134.0, 137.0, 170.0)
val subdoman_length_cuts = Array(0.0, 4.0, 10.0, 15.0)
val entropy_cuts = Array(0.0, 2.0, 3.240223928941852, 3.3735572622751855, 3.506890595608519)
//-----------------------------

val rawdata = sc.textFile(file)
val col = get_column_names(rawdata)

//frame.time,frame.len,ip.src,ip.dst,dns.resp.name,dns.resp.type,dns.resp.class,dns.flags,dns.flags.rcode,dns.a
//Aug  7, 2015 18:33:41.125691000 PDT,97,192.168.1.159,10.0.1.186,dbserver-a301a8c0.int,1,1,33152,0,192.168.1.163
//Aug  7, 2015 18:33:41.135748000 PDT,97,192.168.1.107,10.0.1.200,dbserver-9b01a8c0.int,1,1,33152,0,192.168.1.155
/**
(day,0)
(frame.time,1)
(frame.len,2)
(ip.src,3)
(ip.dst,4)
(dns.resp.name,5)
(dns.resp.type,6)
(dns.resp.class,7)
(dns.flags,8)
(dns.flags.rcode,9)
(dns.a,10)
(domain,11)
(subdomain,12)
(subdomain.length,13)
(domain.freq,14)
(subdomain.entropy,15)
(domain_bin,16)
(frame_length_bin,17)
(subdomain_bin,18)
(word,19)
*/

val datanoheader = removeHeader(rawdata)
val datagood = datanoheader.map(line => line.split(",") ).filter(line => (line.length == 11 | line.length == 10)).map(line => {
    if (line.length == 10){ line :+ "None"
    }else {line}
    })
val databad = datanoheader.filter(line => (line.split(",").length != 11 & line.split(",").length != 10))
//databad.count
//datagood.take(10).foreach(println)
//datagood.count
//res107: Long = 675953


def extract_subdomain(url: String): Array[String ]= {
    var spliturl = url.split("\\.")
    var numparts = spliturl.length
    var domain = "None"
    var subdomain = "None"
    if (spliturl(0) == "http://" | spliturl(0) == "https://" ){
        spliturl = spliturl.slice(1,numparts)
        numparts = numparts - 1
    }
    if (numparts < 4 & numparts > 1){
        domain = spliturl(1)
        subdomain = spliturl(0)
    }else if (numparts> 3){ 
        domain = spliturl(numparts-3)
        subdomain = spliturl.slice(0,numparts-3).mkString(".")
    }else if (numparts>0){ 
        domain = {if (spliturl(0)==""){"None"} else spliturl(0)}
        subdomain = "None"
    }else {domain = {if (spliturl(0)==""){"None"} else spliturl(0)}
        subdomain = "None"
    }
    Array(domain, subdomain, { if (subdomain !="None"){subdomain.length.toString} else {"0"}} )
}


val data_with_subdomains = datagood.map(row => row ++ extract_subdomain(row(col("dns.resp.name")) ) )
col("domain") = col.values.max+1
col("subdomain") = col.values.max+1
col("subdomain.length") = col.values.max+1

//data_with_subdomains.take(200).map(line => line.mkString(",")).foreach(println)

val domain_table = data_with_subdomains.map(row =>(row(col("domain")), 1)).reduceByKey(_+_)
val num_domains = domain_table.map(row => row._2.toDouble).sum()
val domain_table_pct = domain_table.map(row => (row._1, row._2.toDouble/num_domains.toDouble) )
//domain_table_pct.take(num_domains.asInstanceOf[Int]).map(m => m.toString.replaceAll("\\)","").replaceAll("\\(","") ).foreach(println)

val data_with_domain_freq = data_with_subdomains.map(data => (data(col("domain")), data) ).join(domain_table_pct).map( newdata => newdata._2._1 :+ newdata._2._2.toString)
col("domain.freq") = col.values.max+1
 
def entropy( v:String ) : Double = { v
  .groupBy (a => a)
  .values
  .map( i => i.length.toDouble / v.length )
  .map( p => -p * log10(p) / log10(2))
  .sum
}

val data_with_subdomain_entropy = data_with_domain_freq.map(data => data :+ entropy(data(col("subdomain"))).toString )
col("subdomain.entropy") = col.values.max+1

//val domain_freq_cuts = quantiles(quant, compute_ecdf(domain_table_pct.map(r => r._2)))
//val frame_length_cuts = quantiles(quant, compute_ecdf(data_with_subdomains.map(r => r(col("frame.len")).toDouble )))
//val subdoman_length_cuts = quantiles(quant, compute_ecdf(data_with_subdomains.filter(r => r(col("subdomain.length")).toDouble > 0 ).map(r => r(13).toDouble )))
//val entropy_cuts = quantiles(quant, compute_ecdf(data_with_subdomain_entropy.filter(r => r(col("subdomain.entropy")).toDouble > 0 ).map(r => r(15).toDouble )))


def bin_d_f_s(row: Array[String], 
                       domain_freq_cuts: Array[Double], 
                       frame_length_cuts: Array[Double],
                       subdomain_length_cuts: Array[Double]) = {
    val domain_pct = row( col("domain.freq") ).toDouble
    val subdomain_length = row( col("subdomain.length") ).toDouble
    val frame_length = row( col("frame.len") ).toDouble
    var domain_bin = 0
    var subdomain_bin = 0
    var frame_length_bin = 0
    for (cut <- domain_freq_cuts){
        if (domain_pct > cut) { domain_bin = domain_bin+1 }
    }
    for (cut <- frame_length_cuts){
        if (frame_length > cut) { frame_length_bin = frame_length_bin+1 }
    }
    for (cut <- subdomain_length_cuts){
        if (subdomain_length > cut) { subdomain_bin = subdomain_bin+1 }
    }
    row :+ domain_bin.toString :+ frame_length_bin.toString :+ subdomain_bin.toString
}

//s1 = bin_ibyt_ipkt_time(row = s1, ibyt_cuts, ipkt_cuts, time_cuts)
val binned_data = data_with_subdomain_entropy.map(row => bin_d_f_s(row, 
                                                            domain_freq_cuts, 
                                                            frame_length_cuts, 
                                                            subdoman_length_cuts))
col("domain_bin") = col.values.max+1
col("frame_length_bin") = col.values.max+1
col("subdomain_bin") = col.values.max+1



val data_with_words = binned_data.map(row => row :+ row(col("ip.dst")) + "_" + row(col("domain_bin")) + "_" +row(col("frame_length_bin")) + "_" +row(col("subdomain_bin")) )
col("word") = col.values.max+1


val source = data_with_words.map(row => (row(col("ip.dst")), row) )


val topics_lines = sc.textFile(topic_mix_file)
//print(topics_lines)
val words_lines = sc.textFile(pword_file)
//print(words_lines)


val topics = topics_lines.map(line => {
    val ip = line.split(",")(0)
    val text = line.split(",")(1)
    val text_no_quote = text.replaceAll("\"", "")
    (ip, text_no_quote.split(" "))
    })


//topics.take(10)

//this is an inner join, so we only get the ip's that were sources
val src_top = source.join(topics)
//src_top.take(10)

val src_top_w = src_top.map( row => {
    val data = row._2._1
    val topic_mix = row._2._2
    (data(33), (data, topic_mix))
    })

val words = words_lines.map(line => {
    val word = line.split(",")(0)
    val text = line.split(",")(1)
    val text_no_quote = text.replaceAll("\"", "")
    (word, text_no_quote.split(" "))
    })

/**
val words = words_lines.map(line => {
    val word = line.split("\"")(1).replaceAll(",", "_")
    val letters = word.split("_")
    val word_adj = {
        var f : Array[String] = Array() 
        for (letter <- letters){
            if (letter != "-1"){f = f :+ letter + ".0"}
        }
        f.mkString("_")
    }
    val text = line.split("\"")(3)
    (word_adj, text.split(" "))
    })
*/

val src_top_word = src_top_w.join(words)



def wtonum(n: String) = {
    val splits = n.split("e")
    if (splits.length < 2 & splits(0).contains("e") ) { "0"
    }else if (splits.length < 2 & splits(0).toDouble < 1){splits(0)
    }else if (splits.length < 2){"0"
    }else if (splits.length == 2 & (splits(1) == "0" | splits(1) == "-" | splits(1) == "-0") ){"0"
    }else splits(0)+"e"+splits(1)
}


val src_scored = src_top_word.map(row => {
    val data = row._2._1._1
    val topic_mix_orig = row._2._1._2
    val topic_mix = topic_mix_orig.map(n => wtonum(n) )
    val wordprob_orig = row._2._2
    val wordprob = wordprob_orig.map(n => wtonum(n) )
    val src_score = (topic_mix zip wordprob).map(elem => elem._1.toDouble*elem._2.toDouble).reduce(_+_)
    (src_score, data :+ src_score.toString)
    })

//src_scored.take(10)

val scored = src_scored.filter(elem => elem._1 < threshold).repartition(1).sortByKey().map( row => row._2.mkString(",") )
scored.persist(StorageLevel.MEMORY_AND_DISK)
scored.saveAsTextFile(scored_output_file)











//words.take(10)

//val topicsBroadcast = sc.broadcast(topics.collectAsMap())

//val rdd1 = sc.parallelize(Seq((1, "A"), (2, "B"), (3, "C")))
//val rdd2 = sc.parallelize(Seq(((1, "Z"), 111), ((1, "ZZ"), 111), ((2, "Y"), 222), ((3, "X"), 333)))
/**
val rdd1Broadcast = sc.broadcast(rdd1.collectAsMap())
val joined = rdd2.mapPartitions({ iter =>
  val m = rdd1Broadcast.value
  for {
    ((t, w), u) <- iter
    if m.contains(t)
  } yield ((t, w), (u, m.get(t).get))
}, preservesPartitioning = true)
*/





















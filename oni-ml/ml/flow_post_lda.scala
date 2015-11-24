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

def matrixToRdd(m: org.apache.spark.mllib.linalg.Matrix) : org.apache.spark.rdd.RDD[(Long, org.apache.spark.mllib.linalg.DenseVector)] = {
    val cols = m.toArray.grouped(m.numRows)
    val rows = cols.toSeq.transpose
    val vectors = rows.zipWithIndex.map{case (row, index) => (index.toLong, new org.apache.spark.mllib.linalg.DenseVector(row.toArray))}
    sc.parallelize(vectors)
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


val cuts_input = system.getenv("CUT")
var ibyt_cuts : Array[Double] = cuts_input.split(",")(0).split(" ").map(_.toDouble)
var ipkt_cuts : Array[Double] = cuts_input.split(",")(1).split(" ").map(_.toDouble)
var time_cuts : Array[Double] = cuts_input.split(",")(2).split(" ").map(_.toDouble)

//-----------------------------

val rawdata = sc.textFile(file)
//Array(tr, try, trm, trd, trh, trm, trs, td, sa, da, sp, dp, pr, flg, fwd, stos, ipkt, ibyt, opkt, obyt, in, out, sas, das, dtos, dir, ra)
//2015-04-12 00:01:06,2015,4,12,0,1,6,1.340,10.0.121.115,192.168.1.33,80,54048,TCP,.AP.SF,0,0,9,5084,0,0,2,3,0,0,0,0,10.219.32.250

val datanoheader = removeHeader(rawdata)
val sample = datanoheader.sample(false, .0001, 12345)
//val datagood = datanoheader.filter(line => line.split(",").length == 27)
val datagood = sample.filter(line => line.split(",").length == 27)
val databad = datanoheader.filter(line => line.split(",").length != 27)
//var s1 = datagood.first.trim.split(",")

def add_time(row: Array[String]) = {
    val num_time = row(4).toDouble + row(5).toDouble/60 + row(6).toDouble/3600
    row.clone :+ num_time.toString
}

val data_with_time = datagood.map(_.trim.split(",")).map(add_time)
//s1 = add_time(s1)

def bin_ibyt_ipkt_time(row: Array[String], 
                       ibyt_cuts: Array[Double], 
                       ipkt_cuts: Array[Double],
                       time_cuts: Array[Double]) = {
    val time = row(27).toDouble
    val ibyt = row(17).toDouble
    val ipkt = row(16).toDouble
    var time_bin = 0
    var ibyt_bin = 0
    var ipkt_bin = 0
    for (cut <- ibyt_cuts){
        if (ibyt > cut) { ibyt_bin = ibyt_bin+1 }
    }
    for (cut <- ipkt_cuts){
        if (ipkt > cut) { ipkt_bin = ipkt_bin+1 }
    }
    for (cut <- time_cuts){
        if (time > cut) { time_bin = time_bin+1 }
    }
    row.clone :+ ibyt_bin.toString :+ ipkt_bin.toString :+ time_bin.toString
}

//s1 = bin_ibyt_ipkt_time(row = s1, ibyt_cuts, ipkt_cuts, time_cuts)
val binned_data = data_with_time.map(row => bin_ibyt_ipkt_time(row, ibyt_cuts, ipkt_cuts, time_cuts))

def adjust_port(row: Array[String]) = {
    var word_port = 111111.0
    val sip = row(8)
    val dip = row(9)
    val dport = row(10).toDouble
    val sport = row(11).toDouble
    val ipkt_bin = row(29).toDouble
    val ibyt_bin = row(28).toDouble
    val time_bin = row(30).toDouble
    var p_case = 0.0

    var ip_pair = row(9) + " " + row(8)
    if (sip < dip & sip != 0) { ip_pair = row(8) + " " + row(9) }
    
    if ((dport <= 1024 | sport<=1024) & (dport > 1024 | sport>1024) & min(dport,sport) != 0){
        p_case = 2
        word_port = min(dport, sport)
    }else if (dport >1024 & sport>1024){
        p_case = 3
        word_port = 333333
    }else if (dport == 0 & sport != 0){
        word_port = sport
        p_case = 4
    }else if (sport == 0 & dport != 0){
        word_port = dport
        p_case = 4
    }else {
        p_case = 1
        if (min(dport, sport) == 0) {word_port = max(dport, sport)
        }else { word_port = 111111}
    }
    //val word = ipkt_bin -1 + (ibyt_bin-1)*10 + (time_bin - 1)*100 + word_port*1000
    val word = word_port.toString+"_"+time_bin.toString+"_"+ibyt_bin.toString+"_"+ipkt_bin.toString
    var src_word = word
    var dest_word = word
    
    if (p_case == 2 & dport < sport){ dest_word = "-1_"+dest_word
    }else if (p_case == 2 & sport < dport){  src_word = "-1_"+src_word
    }else if (p_case == 4 & dport == 0){  src_word = "-1_"+src_word
    }else if (p_case == 4 & sport ==0){ dest_word = "-1_"+dest_word }
    
    row.clone :+ word_port.toString :+ ip_pair :+ src_word.toString :+ dest_word.toString
}

//s1 = adjust_port(s1)
val data_with_words = binned_data.map(row => adjust_port(row))



val source = data_with_words.map(row => (row(8), row) )
val dest = data_with_words.map(row => (row(9), row) )

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

//Now for destination:
val dest_top = dest.join(topics)

val dest_top_w = dest_top.map( row => {
    val data = row._2._1
    val topic_mix = row._2._2
    (data(33), (data, topic_mix))
    })

val dest_top_word = dest_top_w.join(words)

val dest_scored = dest_top_word.map(row => {
    val data = row._2._1._1
    val topic_mix_orig = row._2._1._2
    val topic_mix = topic_mix_orig.map(n => wtonum(n) )
    val wordprob_orig = row._2._2
    val wordprob = wordprob_orig.map(n => wtonum(n) )
    val dest_score = (topic_mix zip wordprob).map(elem => elem._1.toDouble*elem._2.toDouble).reduce(_+_)
    (dest_score, data :+ dest_score.toString)
    })


//dest_scored.take(10)

val scored = sc.union(src_scored, dest_scored).filter(elem => elem._1 < threshold).repartition(1).sortByKey().map( row => row._2.mkString(",") )
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





















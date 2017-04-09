package rrdagg

/**
  * Created by asen on 4/8/17.
  */
object RRDAggMain {

  private def usageAndExit(): Unit = {
    println("Usage: rrdagg <SUM|SUMN|AVG> <target_rrd> <source_rrd1> <source_rrd2> ...")
    System.exit(1)
  }

  var rrdTool = "rrdtool"

  def main(args: Array[String]): Unit = {
    var myArgs = args.toList
    while (myArgs.nonEmpty && myArgs.head.startsWith("-")){
      val optanme = myArgs.head
      myArgs = myArgs.tail
      if (myArgs.isEmpty){
        usageAndExit()
      }
      val optval = myArgs.head
      if ((optanme == "-r") || (optanme == "--rrdtool"))
        rrdTool = optval
      myArgs = myArgs.tail
    }

    if (myArgs.length < 4) {
      usageAndExit()
    }
    val op = myArgs.head match {
      case "SUM" | "SUMN" | "AVG" => args(0)
      case _ => {
        usageAndExit()
        "SUM" // never
      }
    }
    val targetFile = myArgs(1)
    val sourceFiles = myArgs.drop(2).toList
    println(s"Merging $sourceFiles into $targetFile using $op")
    RRDDump.mergeRrds(rrdTool, op, sourceFiles, targetFile)
    println("Done")
  }

}

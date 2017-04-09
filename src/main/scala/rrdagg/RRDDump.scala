package rrdagg

import scala.io.Source
import scala.xml.parsing.ConstructingParser
import scala.xml.{Comment, Elem, Node, Unparsed}

/**
  * Created by asen on 4/8/17.
  */


class RRDDump(rrdtoolCmd: String, rrdFile: String) {

  type RRDDumpException = RuntimeException

  private val rrdtool = new RRDToolCommand(rrdtoolCmd)
  private val rawOutput = rrdtool.dump(rrdFile).mkString("\n")
//  val parsedOutput: Elem = ConstructingParser.fromSource(Source.fromString(rawOutput), false).document().head

//  println(parsedOutput)
//  println(parsedOutput.head)
//  println(parsedOutput.head.label)

  val rootNode: Node = ConstructingParser.fromSource(Source.fromString(rawOutput), preserveWS = false).document().docElem.head

  if (rootNode.label != "rrd")
    throw new RRDDumpException(s"invalid xml from $rrdFile")

//  println(rootNode.toString().take(100))
//  println(rootNode.child.map(_.label).toString().take(100))

  val step: Int = getChildText(rootNode, "step").toInt
  val lastUpdate: Int = getChildText(rootNode, "lastupdate").toInt

  val (dss, dspdps) = rootNode.child.filter(_.label == "ds").toList.map { n =>
    (
      RRDDs(
        getChildText(n, "name"),
        getChildText(n, "type"),
        getChildText(n, "minimal_heartbeat").toInt,
        getChildText(n, "min").toDouble,
        getChildText(n, "max").toDouble
      ),
      RRDDsPdp(
        getChildText(n, "last_ds").toDouble,
        getChildText(n, "value").toDouble,
        getChildText(n, "unknown_sec").toInt
      )
    )
  }.unzip

  if (dss.isEmpty)
    throw new RRDDumpException(s"no dss in $rrdFile")

//  println(dss)

  val rras: List[RRDRra] = rootNode.child.filter(_.label == "rra").toList.map { n =>
    val db = n.child.find(_.label == "database").get
    val rows = db.child.toList.grouped(2).map {seq =>
      val comment = seq(0).asInstanceOf[Comment].toString()
      val ts = comment.split("/").last.trim.split("[^\\d]").head.trim.toInt
      val rowNode = seq(1)
      if (rowNode.label != "row")
        throw new RRDDumpException(s"row node has wrong label: ${rowNode.label}, $rowNode")
      val vals = rowNode.child.filter(_.label == "v").map(_.text.toDouble).toList
      RRDRow(ts, vals)
    }.toList
    val cf = getChildText(n, "cf")
    val ppr = getChildText(n, "pdp_per_row").toInt
    val paramsXff = n.child.find(_.label == "params").map { pn =>
      getChildText(pn, "xff").toDouble
    }.get

    val cdpPrep = n.child.find(_.label == "cdp_prep").map { pn =>
      pn.child.filter(_.label == "ds").toList.map { dsn =>
        RRDCdpPrepDs(
          getChildText(dsn, "primary_value").toDouble,
          getChildText(dsn, "secondary_value").toDouble,
          getChildText(dsn, "value").toDouble,
          getChildText(dsn, "unknown_datapoints").toInt
        )
      }
    }.get
    RRDRra(cf, ppr, paramsXff, cdpPrep, rows)
  }

//  println(rras.size)

  val parsed = RRDFile(rrdFile, step, lastUpdate, dss, dspdps, rras)

  private def getChildText(parent: Node, label: String) = {
    val retOpt = parent.child.find(_.label == label).map(_.text)
    if (retOpt.isEmpty)
      throw new RRDDumpException(s"Error with $rrdFile. Could not find child with label $label (parent label = ${parent.label})")
    retOpt.get
  }

}

object RRDDump {

  def mergeVals(op: String, toMerge: List[List[Double]]): List[Double] = {
    val nanAs0 = op.endsWith("N")
    toMerge.head.zipWithIndex.map { case (init, idx) =>
      val toOp = toMerge.map(l => l(idx)).map { d =>
        op match {
          case "SUMN" => Some(if (d.isNaN) 0.0 else d)
          case "AVG" => if (d.isNaN) None else Some(d)
          case _ => Some(d)
        }
      }.collect { case Some(x) => x }
      if (toOp.isEmpty) {
        Double.NaN
      } else {
        val sum = toOp.sum
        if (op.startsWith("AVG"))
          sum / toOp.size
        else sum
      }
    }
  }

  def mergePdps(op: String, src: List[RRDFile]): List[RRDDsPdp] = {
    //TODO
    src.head.dsPdps.map(_ => RRDDsPdp.unknown)
  }

  def mergeCdpPreps(op: String, src: List[List[RRDCdpPrepDs]]): List[RRDCdpPrepDs] = {
    //TODO
    src.head.map { h =>
      RRDCdpPrepDs.nan(h.unknownDatapoints)
    }
  }

  def mergeRras(op: String, src: List[RRDFile]): List[RRDRra] = {
    val rras = src.head.rras.zipWithIndex.map { case (hrra, rraIx) =>
      val tailByTs = src.tail.map { rf => rf.rras(rraIx).database.groupBy(_.ts).map(t => (t._1, t._2.head)) }
      val rows = hrra.database.map { hrow =>
        val toMerge = List(hrow.vals) ++ tailByTs.map( byTs => byTs.get(hrow.ts)).map { opt =>
          if (opt.isEmpty) {
            hrow.vals.map(_ => Double.NaN)
          } else opt.get.vals
        }
        val merged = mergeVals(op, toMerge)
        RRDRow(hrow.ts, merged)
      }
      val cdpsToMerge = List(hrra.cdpPrep) ++ src.tail.map{ rf => rf.rras(rraIx).cdpPrep }
      RRDRra(hrra.cf, hrra.pdpPerRow, hrra.paramsXff, mergeCdpPreps(op, cdpsToMerge), rows)
    }
    rras
  }

  def mergeRrds(rrdtool: String, op: String, sources: List[String], tgt: String): Unit = {
    val dumps = sources.map(s => new RRDDump(rrdtool, s).parsed)
    dumps.tail.foreach { d =>
      if (!dumps.head.canMerge(d)){
        throw new RuntimeException(s"Incompatible rrd file: ${d.srcfn}")
      }
    }
    val mergedFile = RRDFile(tgt, dumps.head.step,
      dumps.head.lastUpdate, dumps.head.dss, mergePdps(op, dumps), mergeRras(op, dumps))
    val rrdt = new RRDToolCommand(rrdtool)
    rrdt.restore(mergedFile.toXml.toString(), tgt)
  }


}

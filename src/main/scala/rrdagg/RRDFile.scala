package rrdagg

import scala.xml.{Elem, Unparsed}

/**
  * Created by asen on 4/9/17.
  */
case class RRDDsPdp(lastDs: Double, value: Double, unknownSec: Int)

object RRDDsPdp {
  val unknown = RRDDsPdp(Double.NaN, Double.NaN, 0)
}

case class RRDDs(name: String, rrdType: String, minimalHeartbeat: Int, min: Double, max: Double) {

  def toXml(withPdp: RRDDsPdp): Elem = {
    <ds>
      <name> {name} </name>
      <type> {rrdType} </type>
      <minimal_heartbeat>{minimalHeartbeat}</minimal_heartbeat>
      <min>{min}</min>
      <max>{max}</max>

      <!-- PDP Status -->
      <last_ds>{withPdp.lastDs}</last_ds>
      <value>{withPdp.value}</value>
      <unknown_sec> {withPdp.unknownSec} </unknown_sec>
    </ds>
  }
}

case class RRDRow(ts: Int, vals: List[Double]) {

  //  <!-- 2017-04-03 20:49:00 EEST / 1491241740 --> <row><v>NaN</v><v>NaN</v><v>NaN</v></row>
  def toXml: Elem = {

    <row>
      {
      vals.map { v =>
        <v>{v}</v>
      }
      }
    </row>

  }
}

case class RRDCdpPrepDs(primaryValue: Double, secondaryValue: Double, value: Double, unknownDatapoints: Int){

  def toXml: Elem = {
    <ds>
      <primary_value>{primaryValue}</primary_value>
      <secondary_value>{secondaryValue}</secondary_value>
      <value>{value}</value>
      <unknown_datapoints>{unknownDatapoints}</unknown_datapoints>
    </ds>
  }
}

object RRDCdpPrepDs {
  def nan(withUnknownDatapoints: Int) = RRDCdpPrepDs(Double.NaN, Double.NaN, Double.NaN, withUnknownDatapoints)
}

case class RRDRra(
                   cf: String,
                   pdpPerRow: Int,
                   paramsXff: Double,
                   cdpPrep: List[RRDCdpPrepDs],
                   database: List[RRDRow]
                 ) {
  def toXml: Elem = {
    <rra>
      <cf>{cf}</cf>
      <pdp_per_row>1</pdp_per_row> <!-- 60 seconds -->

      <params>
        <xff>{paramsXff}</xff>
      </params>
      <cdp_prep>
        {
        cdpPrep.map {
          _.toXml
        }
        }
      </cdp_prep>
      <database>
        {
        database.map {
          _.toXml
        }
        }
      </database>
    </rra>
  }
}

case class RRDFile(
                    srcfn: String,
                    step: Int,
                    lastUpdate: Int,
                    dss: List[RRDDs],
                    dsPdps: List[RRDDsPdp],
                    rras: List[RRDRra]
                  ) {
  //     <?xml version="1.0" encoding="utf-8"?>
  //  <!DOCTYPE rrd SYSTEM "http://oss.oetiker.ch/rrdtool/rrdtool.dtd">
  //    <!-- Round Robin Database Dump -->

  def toXml: Elem = {
    <rrd>
      <version>0003</version>
      <step>{step}</step> <!-- Seconds -->
      <lastupdate>{lastUpdate}</lastupdate> <!-- 2017-04-07 20:48:00 EEST -->

      {
      dss.zip(dsPdps).map { t =>
        t._1.toXml(t._2)
      }
      }

      <!-- Round Robin Archives -->
      {
      rras.map(_.toXml)
      }

    </rrd>
  }

  def canMerge(oth: RRDFile): Boolean = {
    step == oth.step &&
    dss.size == oth.dss.size &&
    dss.zip(oth.dss).forall(t => t._1.rrdType == t._2.rrdType) &&
    rras.size == oth.rras.size &&
    rras.zip(oth.rras).forall(t => t._1.cf == t._2.cf && t._1.pdpPerRow == t._2.pdpPerRow )
  }
}

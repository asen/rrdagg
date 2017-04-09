package rrdagg

import java.io.ByteArrayInputStream

import scala.sys.process.{Process, ProcessLogger, _}

/**
  * Created by asen on 4/8/17.
  */
class RRDToolCommand(rrdtool: String) {

  def dump(rrdfile: String): List[String] = {
    val cmd = s"$rrdtool dump $rrdfile"
    runCommand(cmd)
  }

  def restore(xmlStr: String, outRrd: String): Unit ={
    runCommand(s"$rrdtool restore - $outRrd", Some(xmlStr))
  }

  def runCommand(cmd: String, stdin: Option[String] = None): List[String] = {
    val cmdSeq = Seq("bash", "-c") ++ Seq(cmd)
    println("RUN_COMMAND: " + cmdSeq)
    val qb = Process(cmdSeq, None)
    var out = List[String]()
    var err = List[String]()

    val exit = if (stdin.isEmpty)
      qb ! ProcessLogger((s) => out ::= s, (s) => err ::= s)
    else
      qb #< new ByteArrayInputStream(stdin.get.getBytes) ! ProcessLogger((s) => out ::= s, (s) => err ::= s)

    if (exit != 0)
      throw new RuntimeException(s"Command failed: $cmdSeq EXIT_CODE: $exit STDERR: \n" + err.reverse.mkString("\n"))
    out.reverse
  }
}

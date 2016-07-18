package com.cloudera.sa.apptrans.server.hbase

import java.io.File

import com.sun.jersey.spi.container.servlet.ServletContainer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{Context, ServletHolder}

object HBaseRestServer {
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<port> <configDir> <numberOfSalts> <customerTableName>")
    }
    val port = args(0).toInt
    val hbaseConfigFolder = args(1)
    val numberOfSalts = args(2).toInt
    val customerTableName = args(3)

    val conf = HBaseConfiguration.create()
    conf.addResource(new File(hbaseConfigFolder + "hbase-site.xml").toURI.toURL)

    HBaseGlobalValues.init(conf, numberOfSalts, customerTableName)

    val server = new Server(port)

    val sh = new ServletHolder(classOf[ServletContainer])
    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig")
    sh.setInitParameter("com.sun.jersey.config.property.packages", "com.cloudera.sa.example.card.server.hbase")
    sh.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true")

    val context = new Context(server, "/", Context.SESSIONS)
    context.addServlet(sh, "/*")

    println("starting HBase Rest Server")
    server.start()
    println("started HBase Rest Sserver")
    server.join()
  }
}

package com.cloudera.sa.apptrans.server.kudu

import com.sun.jersey.spi.container.servlet.ServletContainer
import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{Context, ServletHolder}

object KuduRestServer {
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<port> <KuduMaster> <customerTransTableName> <customerMartTableName>")
    }

    val port = args(0).toInt
    val kuduMaster = args(1)
    val customerTransTableName = args(2)
    val customerMartTableName = args(3)

    KuduGlobalValues.init(kuduMaster,customerTransTableName, customerMartTableName)

    val server = new Server(port)

    val sh = new ServletHolder(classOf[ServletContainer])
    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig")
    sh.setInitParameter("com.sun.jersey.config.property.packages", "com.cloudera.sa.example.card.server.kudu")
    sh.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true")

    val context = new Context(server, "/", Context.SESSIONS)
    context.addServlet(sh, "/*")

    println("starting Kudu Rest Server")
    server.start()
    println("started Kudu Rest Server")
    server.join()
  }
}

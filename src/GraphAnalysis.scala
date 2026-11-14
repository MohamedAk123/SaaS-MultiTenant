import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._

object GraphAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Jalon2-Partie6-GraphX")
      .master("spark://spark-master:7077")
      .config("spark.driver.host", "jupyter").config("spark.driver.bindAddress", "0.0.0.0")
      .config("spark.executor.memory", "1g").config("spark.executor.cores", "2")
      .config("spark.cores.max", "4").config("spark.sql.shuffle.partitions", "4")
      .config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", "file:///opt/spark/events")
      .config("spark.hadoop.fs.s3a.endpoint", sys.env("S3_ENDPOINT"))
      .config("spark.hadoop.fs.s3a.access.key", sys.env("MINIO_ROOT_USER"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env("MINIO_ROOT_PASSWORD"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val base = "s3a://lake/gold/graph_analysis"
    try {
      val vertices = spark.read.parquet(base+"/vertices").cache()
      val edges = spark.read.parquet(base+"/edges").cache()
      val srcMap = vertices.select($"id".as("src"), $"vertex_id".as("src_id"))
      val dstMap = vertices.select($"id".as("dst"), $"vertex_id".as("dst_id"))
      val numbered = edges.join(srcMap,"src").join(dstMap,"dst").cache()
      val tenantIds = vertices.select("tenant_id").distinct().as[String].collect().sorted
      var allResults = Seq.empty[(String,String,Long,Int,Long,Double)]
      var timings = Seq.empty[(String,Double,Double,Double)]
      tenantIds.foreach { tenant =>
        val vs = vertices.filter($"tenant_id"===tenant).select("vertex_id","id").rdd.map(r => (r.getLong(0), r.getString(1)))
        val es = numbered.filter($"tenant_id"===tenant).select("src_id","dst_id").rdd.map(r => Edge(r.getLong(0),r.getLong(1),1))
        val graph = Graph(vs,es,"").cache()
        graph.vertices.count(); graph.edges.count()
        var start = System.nanoTime()
        val deg = graph.degrees.collect().toMap
        val degreeTime = (System.nanoTime()-start)/1e9
        start = System.nanoTime()
        val cc = graph.connectedComponents().vertices.collect().toMap
        val ccTime = (System.nanoTime()-start)/1e9
        start = System.nanoTime()
        val pr = graph.pageRank(0.000001).vertices.collect().toMap
        val prTime = (System.nanoTime()-start)/1e9
        allResults ++= graph.vertices.collect().map { case (vid,id) => (tenant,id,vid,deg.getOrElse(vid,0),cc(vid),pr(vid)) }
        timings :+= (tenant,degreeTime,ccTime,prTime)
        graph.unpersist()
      }
      val result = allResults.toDF("tenant_id","id","vertex_id","degree","component","pagerank")
      result.write.mode("overwrite").partitionBy("tenant_id").parquet(base+"/graphx")
      result.orderBy(desc("pagerank")).show(20,false)
      timings.toDF("tenant_id","degree_seconds","connected_components_seconds","pagerank_seconds")
        .coalesce(1).write.mode("overwrite").json("/workspace/results/graphx_timings")
      result.coalesce(1).write.mode("overwrite").json("/workspace/results/graphx_results")
      println("GRAPHX_APPLICATION_ID="+spark.sparkContext.applicationId)
    } finally { spark.stop() }
  }
}

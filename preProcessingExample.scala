// Settings
val labels = Seq("1", "2")
val init_weights = Map("1" -> .5, 
                       "2" -> .5
                      )

// Load in textFile representation of the Graph
val nodes: RDD[String] = sc.textFile("./notebooks/cme/afe1-noise/vertexlist600n20e2tNOISE.tsv")
val links: RDD[String] = sc.textFile("./notebooks/cme/afe1-noise/edgelist600n20e2tNOISE.tsv")

// Map Vertices To (VertexId, TrueRank)
val vertices = nodes.map { line =>
  val fields = line.split('\t')
  (fields(0).toLong, fields(1))
}

// Map Edges to (Src, Dst, Type)
val edges = links.map { line =>
  val fields = line.split('\t')
  Edge(fields(0).toLong, fields(1).toLong, fields(2).toString)
}

/* Create a set of ([EdgeType], [Count]) so that each vertex 
   can calculate it's denominator given a set of weights. */
val edgeCounts = edges.map(e => 
  ( (e.srcId, e.attr), 1) ).reduceByKey((a,b) => a + b)
  .map{case ((vertex, edgeType), count) => 
       (vertex, Set((edgeType, count)))}.reduceByKey((a,b) => a ++ b)



/* Construct the graph by initiating vertices and edges and joining
   with the edge counts. This will create a structured graph that
   we can reuse in each iteration of the Weighted PageRank. */
val structuredGraph = Graph(vertices, edges).outerJoinVertices(edgeCounts){
                        (vid, vd, edgeCounts) => 
                          (vd.toDouble, edgeCounts.getOrElse(Set()))
                        }

// edgeCounts.take(5).foreach(println)
// structuredGraph.vertices.sortBy(_._1).take(5).foreach(println)

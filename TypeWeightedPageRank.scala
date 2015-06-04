import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Set
import scala.reflect.ClassTag
import scala.language.postfixOps
import scala.util.Random

 /* TypeWeightedPageRank is a modified version of the Spark
    PageRank implementation.
    
    - instead of taking in a Graph[VD, ED], this takes a 
      Graph[(Double, Set[(String, Int)]), String] which contains
      both the TrueRank and a Set that contains the structure
      of the Graph.
      
    - the edges of this graph must be initiated to the edge type
    
    - we pass in the edge type weights as a Map
    
    - using the weight map, each vertex calculates the total weight 
      associated with it's outgoing edges
      
    - then, each edge calculate its weight via .mapTriplets
    
    - the PageRank Graph that's calculated has vertices that 
      contains a tuple (TrueRank, WeightedPageRankScore)
*/
def TypeWeightedPageRank[VD: ClassTag, ED: ClassTag](
      graph: Graph[(Double, Set[(String, Int)]), String], 
      tol: Double, 
      resetProb: Double = 0.15,
      weights: Map[String,Double]) : Graph[(Double, Double), Double] =
  {
    val pagerankGraph: Graph[(Double, Double, Double), Double] = graph
      // Calculate the total weight associated with each vertex
      .mapVertices{case (vertexId, (s, attr)) =>
                     // prevent divide by zero errors
                     if (attr.isEmpty) (s, 1)
                     // otherwise, calculate the denominator
                     else (s, attr.map{case (edgeType, count) => 
                                        weights(edgeType)*count}
                                  .reduce((a,b) =>  a + b))
                  }
      // Set the edge weight based on the edge type and its source vertex degree
      .mapTriplets(e => (weights(e.attr).toDouble/e.srcAttr._2.asInstanceOf[Number].doubleValue))
      // Set the vertex attributes to (initalPR, delta = 0, trueScore)
      .mapVertices( (id, attr) => (0.0, 0.0, attr._1) )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double,Double), msgSum: Double): (Double, Double, Double) = {
      val (oldPR, lastDelta, trueScore) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR, trueScore)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    val vp = (id: VertexId, attr: (Double, Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)

    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => (attr._1, attr._3) )
  }
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Set
import scala.reflect.ClassTag
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer
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

/* Objective Function */
  def calcSquaredError(scores:VertexRDD[(Double,Double)]): Double = {
    /* Collect the VertexRDD,
       sort it by calculated PageRank, 
       assign it a ranking,
       calculate the sum of squared rank differences */
       
   val error = scores.sortBy(_._2._1, ascending=false)
                      .zipWithIndex()
                      .map{case ( (nodeId, (trueRank, pageRank)), observedRank) =>
                           math.pow(trueRank - observedRank - 1, 2)}
                      .reduce((a,b) => a + b)

    error

}

def computeGradient(currentPoint:Map[String,Double],
                    assignedGraph:Graph[(Double, Set[(String,Int)]), String],
                    delta:Double) = {
    // Calculate the Rank Correlation at the starting parameter
  val currentPR = scoredTypeWeightedPageRankWTruth(assignedGraph, 0.01, 0.15, weights = currentPoint)
  val currentError = calcSquaredError(currentPR.vertices)
  
  val labels = currentPoint.toSeq.sortBy(_._1).map(_._1)
  val currentWeight = currentPoint.toSeq.sortBy(_._1).map(_._2)
  
  val lastIndex = currentWeight.length - 1
  var gradient :ArrayBuffer[Double] = ArrayBuffer.fill[Double](currentWeight.length-1)(0)
  
  for (i <- 0 to lastIndex-1){
    
    if (currentWeight(i) + delta > 1){
       println("Hello, we're screwed.", currentWeight(i) + delta)
    }  

//    val currentWeight = currentPoint.toSeq.sortBy(_._1).map(_._2)
    val newWeight = currentWeight.updated(i, currentWeight(i) + delta)
                                 .updated(lastIndex, 
                                          math.max(currentWeight(lastIndex) - delta, 0))
    
    val newPoint = Map(labels.zip(newWeight): _*)
    
    val newPR = scoredTypeWeightedPageRankWTruth(assignedGraph, 0.01, 0.15, weights = newPoint)
    
    gradient = gradient.updated(i, (calcSquaredError(newPR.vertices) - currentError)/delta)
    //println(newWeight)
  }  
  (gradient, currentError)
}

def descent( assignedGraph:Graph[(Double,Set[(String, Int)]), String],
             delta:Double,
             stepSize:Double,
             labels:ArrayBuffer[String],
             currentPoint:Map[String,Double]) = {

  val currentWeight = currentPoint.toSeq.sortBy(_._1).map(_._2)
  
  
  val temp = computeGradient(currentPoint, assignedGraph, delta=.001)
  val currentGradient = temp._1  ++ ArrayBuffer(0)
  val currentError = temp._2
  
  
  val tempWeight = currentWeight.zip(currentGradient).map(x => x._1 - x._2.asInstanceOf[Number].doubleValue)
  val newWeight = tempWeight.slice(0, labels.length-1) ++ ArrayBuffer(Math.max(1-tempWeight.slice(0, labels.length-1).sum, 0))

  // println(newWeight)
  
  val normalizedWeight = newWeight.map(x => x/newWeight.sum)
  
  (Map(labels.zip(normalizedWeight):_*), currentError)
}

def gradientDescent( assignedGraph:Graph[(Double,Set[(String, Int)]), String],
                     delta:Double,
                     stepSize:Double,
                     labels:ArrayBuffer[String],
                     currentPoint:Map[String,Double],
                     conv_tol:Double) = {
  var startPoint = currentPoint
  
  var newError = 0.0
  var delta = 1.0
  var oldError = 0.0
  var iterations = 0
  
  while(delta > conv_tol) {
        val temp  = descent(assignedGraph:Graph[(Double,Set[(String, Int)]), String],
                             delta:Double,
                             stepSize:Double,
                             labels:ArrayBuffer[String],
                             startPoint:Map[String,Double])
        startPoint = temp._1
    
        newError = temp._2
        delta = Math.abs(newError - oldError)
        oldError = newError
        iterations += 1
        println(iterations, startPoint, delta) 
  }
  (startPoint, iterations)
}

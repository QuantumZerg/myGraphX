import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val graphPR = GraphLoader.edgeListFile(sc, "graphx/data/followers.txt")
val Num = graphPR.numVertices
val d = 0.85

val damp = (1.0 - d)/ Num
val reset = 0.15
val tol = 0.0001

val initialGraphPR: Graph[(Double, Double), Double] = graphPR.outerJoinVertices(graphPR.outDegrees){ (id, vdata, outdeg) => outdeg.getOrElse(0)}.mapTriplets(tr => 1.0 / tr.srcAttr).mapVertices((id, vdata) => (0.0, 0.0)).cache() 

def receiveMessage(id: VertexId, vdata: (Double, Double),  msg: Double): (Double, Double) ={
  val (curData, diff) = vdata
  val newData = curData + (1 - reset) * msg 
  (newData, newData - curData)
}

def sendMessage(tr: EdgeTriplet[(Double,Double), Double]) = {
  if(tr.srcAttr._2 > tol){
    Iterator((tr.dstId, tr.srcAttr._2 * tr.attr)) // message = vData / outdeg
  }else{
    Iterator.empty
  }
}

def combiner(a: Double, b: Double): Double = a + b

val initialMessage = reset / (1.0 - reset)

val ranks = initialGraphPR.pregel(initialMessage)(receiveMessage, sendMessage, combiner).vertices
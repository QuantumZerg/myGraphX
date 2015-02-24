import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val graphPR = GraphLoader.edgeListFile(sc, "graphx/data/followers.txt")
val Num = graphPR.numVertices
val d = 0.85
val mgi = 1.0 / Num
val damp = (1.0 - d)/ Num
val tol = 0.0001

val initialGraphPR: Graph[(Double, Double), Double] = graphPR.outerJoinVertices(graphPR.outDegrees){ (id, vdata, outdeg) => outdeg match{
    case Some(outdeg) => d / outdeg
    case None => 0.0
    }
}.mapTriplets(tr => tr.srcAttr + damp ).mapVertices((id, vdata) => (0.0, 0.0)).cache() //initilize all edge as d/outdeg of src, 

def receiveMessage(id: VertexId, vdata: (Double, Double),  msg: Double): (Double, Double) ={
  val (curData, diff) = vdata
  val newData = damp * curData + msg // updating according to the rule of (1 - d)/N + d*(vData / outdeg)
  (newData, Math.abs(newData - curData))
}

def sendMessage(tr: EdgeTriplet[(Double,Double), Double]) = {
  if(tr.srcAttr._2 > tol){// there might be something wrong!!!
    Iterator((tr.dstId, tr.srcAttr._1 * tr.attr)) // where message = vData / outdeg
  }else{
    Iterator.empty
  }
}

def combiner(a: Double, b: Double): Double = a + b

val ranks = initialGraphPR.pregel(mgi)(receiveMessage, sendMessage, combiner).vertices
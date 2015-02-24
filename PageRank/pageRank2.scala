import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val graphPR = GraphLoader.edgeListFile(sc, "graphx/data/followers.txt")
val Num = graphPR.numVertices
val d = 0.85
val mgi = 0.0
val damp = (1.0 - d)

val initialGraphPR: Graph[Double, Double] = graphPR.outerJoinVertices(graphPR.outDegrees){ (id, vdata, outdeg) => outdeg match{
    case Some(outdeg) => 1.0 / outdeg
    case None => 0.0
    }
}.mapTriplets(tr => tr.srcAttr).mapVertices((id, vdata) => 1.0).cache()

def receiveMessage(id: VertexId, vdata: Double, msg: Double): Double = damp + msg * d

def sendMessage(tr: EdgeTriplet[Double, Double]) = Iterator((tr.dstId, tr.srcAttr * tr.attr))

def combiner(a: Double, b: Double): Double = a + b

val ranks = initialGraphPR.pregel(mgi, 30)(receiveMessage, sendMessage, combiner).vertices


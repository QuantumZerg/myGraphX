val graphPR = GraphLoader.edgeListFile(sc, "graphx/data/followers.txt")
val Num = graphPR.numVertices
val d = 0.85
val mgi = d / Num
val damp = (1.0 - d)/ Num

val initialGraphPR: Graph[Double, Double] = graphPR.outerJoinVertices(graphPR.outDegrees){ (id, vdata, outdeg) => outdeg match{
    case Some(outdeg) => 1.0 / outdeg
    case None => 0.0
    }
}.mapTriplets(tr => tr.srcAttr).mapVertices((id, vdata) => 1.0 / Num)

def receiveMessage(id: VertexId, vdata: Double, msg: Double): Double = damp + msg

def sendMessage(tr: EdgeTriplet[Double, Double]) = Iterator((tr.dstId, tr.srcAttr * tr.attr*d))

def combiner(a: Double, b: Double): Double = a + b

val ranks = initialGraphPR.pregel(mgi, 30)(receiveMessage, sendMessage, combiner).vertices


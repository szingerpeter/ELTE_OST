package project

/*

Note: The Denstream class is mainly based upon the "WithDBSCAN" class from the moa package
Link: https://github.com/Waikato/moa

 */

import com.github.javacliparser.{FloatOption, IntOption}
import com.yahoo.labs.samoa.instances.{DenseInstance, Instance}
import moa.cluster.{Cluster, Clustering}
import moa.clusterers.AbstractClusterer
import moa.clusterers.`macro`.dbscan.DBScan
import moa.clusterers.denstream._
import moa.core

import java.{lang, util}

@SerialVersionUID(1L)
class DenPoint(val nextInstance: Instance, val timestamp: Long) extends DenseInstance(nextInstance) {
    this.setDataset(nextInstance.dataset)
    var covered: Boolean = false
}

@SerialVersionUID(1L)
class DenStream extends AbstractClusterer {
    var horizonOption: IntOption = new IntOption("horizon", 'h', "Range of the window.", 1000)
    var epsilonOption = new FloatOption("epsilon", 'e', "Defines the epsilon neighbourhood", 0.02D, 0.0D, 0.5D)
    var betaOption: FloatOption = new FloatOption("beta", 'b', "", 0.2D, 0.0D, 1.0D)
    var muOption: FloatOption = new FloatOption("mu", 'm', "", 1.0D, 0.0D, 1.7976931348623157E308D)
    var initPointsOption = new IntOption("initPoints", 'i', "Number of points to use for initialization.", 25)
    var offlineOption: FloatOption = new FloatOption("offline", 'o', "offline multiplier for epsilion.", 2.0D, 2.0D, 20.0D)
    var lambdaOption = new FloatOption("lambda", 'l', "", 0.25D, 0.0D, 0.5D)
    var speedOption = new IntOption("processingSpeed", 's', "Number of incoming points per time unit.", 3, 1, 25)
    private val weightThreshold: Double = 0.01D
    private var lambda: Double = .0
    private var epsilon: Double = .0
    private var minPoints: Int = 0
    private var mu: Double = .0
    private var beta: Double = .0
    private var p_micro_cluster: Clustering = null
    private var o_micro_cluster: Clustering = null
    private var initBuffer: util.ArrayList[DenPoint] = null
    private var initialized: Boolean = false
    private var timestamp: Long = 0L
    private var currentTimestamp: Timestamp = null
    private var tp: Long = 0L
    protected var numInitPoints: Int = 0
    protected var numProcessedPerUnit: Int = 0
    protected var processingSpeed: Int = 0
    
    override def resetLearningImpl(): Unit = {
        this.currentTimestamp = new Timestamp
        this.lambda = this.lambdaOption.getValue
        this.epsilon = this.epsilonOption.getValue
        this.minPoints = this.muOption.getValue.toInt
        this.mu = this.muOption.getValue.toInt.toDouble
        this.beta = this.betaOption.getValue
        this.initialized = false
        this.p_micro_cluster = new Clustering
        this.o_micro_cluster = new Clustering
        this.initBuffer = new util.ArrayList()
        this.tp = (1.0D / this.lambda * Math.log(this.beta * this.mu / (this.beta * this.mu - 1.0D)).round + 1L).toLong
        this.numProcessedPerUnit = 0
        this.processingSpeed = this.speedOption.getValue
    }
    
    def initialDBScan(): Unit = {
        for (p <- 0 until this.initBuffer.size) {
            val point: DenPoint = this.initBuffer.get(p).asInstanceOf[DenPoint]
            if (!point.covered) {
                point.covered = true
                val neighbourhood: util.ArrayList[Integer] = this.getNeighbourhoodIDs(point, this.initBuffer, this.epsilon)
                if (neighbourhood.size > this.minPoints) {
                    val mc: MicroCluster = new MicroCluster(point, point.numAttributes, this.timestamp, this.lambda, this.currentTimestamp)
                    this.expandCluster(mc, this.initBuffer, neighbourhood)
                    this.p_micro_cluster.add(mc)
                }
                else point.covered = false
            }
        }
    }
    
    override def trainOnInstanceImpl(inst: Instance): Unit = {
        val point: DenPoint = new DenPoint(inst, this.timestamp)
        this.numProcessedPerUnit += 1
        if (this.numProcessedPerUnit % this.processingSpeed == 0) {
            this.timestamp += 1
            this.currentTimestamp.setTimestamp(this.timestamp)
        }
        if (!this.initialized) {
            this.initBuffer.add(point)
            if (this.initBuffer.size >= this.initPointsOption.getValue) {
                this.initialDBScan()
                this.initialized = true
            }
        }
        else {
            var merged: Boolean = false
            var x: MicroCluster = null
            var xCopy: MicroCluster = null
            if (this.p_micro_cluster.getClustering.size != 0) {
                x = this.nearestCluster(point, this.p_micro_cluster)
                xCopy = x.copy
                xCopy.insert(point, this.timestamp)
                if (xCopy.getRadius(this.timestamp) <= this.epsilon) {
                    x.insert(point, this.timestamp)
                    merged = true
                }
            }
            if (!merged && this.o_micro_cluster.getClustering.size != 0) {
                x = this.nearestCluster(point, this.o_micro_cluster)
                xCopy = x.copy
                xCopy.insert(point, this.timestamp)
                if (xCopy.getRadius(this.timestamp) <= this.epsilon) {
                    x.insert(point, this.timestamp)
                    merged = true
                    if (x.getWeight > this.beta * this.mu) {
                        this.o_micro_cluster.getClustering.remove(x)
                        this.p_micro_cluster.getClustering.add(x)
                    }
                }
            }
            if (!merged) this.o_micro_cluster.getClustering.add(new MicroCluster(point.toDoubleArray, point.toDoubleArray.length, this.timestamp, this.lambda, this.currentTimestamp))
            if (this.timestamp % this.tp == 0L) {
                val removalList: util.ArrayList[MicroCluster] = new util.ArrayList()
                var var16 = this.p_micro_cluster.getClustering.iterator()
                var c: Cluster = null
                while ( {
                    var16.hasNext
                }) {
                    c = var16.next.asInstanceOf[Cluster]
                    if (c.asInstanceOf[MicroCluster].getWeight < this.beta * this.mu) removalList.add(c.asInstanceOf[MicroCluster])
                }
                var16 = removalList.iterator().asInstanceOf[java.util.Iterator[Cluster]]
                while ( {
                    var16.hasNext
                }) {
                    c = var16.next.asInstanceOf[Cluster]
                    this.p_micro_cluster.getClustering.remove(c)
                }
                var16 = this.o_micro_cluster.getClustering.iterator()
                while ( {
                    var16.hasNext
                }) {
                    c = var16.next.asInstanceOf[Cluster]
                    val t0: Long = c.asInstanceOf[MicroCluster].getCreationTime
                    val xsi1: Double = Math.pow(2.0D, -this.lambda * (this.timestamp - t0 + this.tp).toDouble) - 1.0D
                    val xsi2: Double = Math.pow(2.0D, -this.lambda * this.tp.toDouble) - 1.0D
                    val xsi: Double = xsi1 / xsi2
                    if (c.asInstanceOf[MicroCluster].getWeight < xsi) removalList.add(c.asInstanceOf[MicroCluster])
                }
                var16 = removalList.iterator().asInstanceOf[java.util.Iterator[Cluster]]
                while ( {
                    var16.hasNext
                }) {
                    c = var16.next.asInstanceOf[Cluster]
                    this.o_micro_cluster.getClustering.remove(c)
                }
            }
        }
    }
    
    private def expandCluster(mc: MicroCluster, points: util.ArrayList[DenPoint], neighbourhood: util.ArrayList[Integer]): Unit = {
        val var4: Iterator[_] = neighbourhood.iterator().asInstanceOf[scala.collection.Iterator[_]]
        while ( {
            var4.hasNext
        }) {
            val p: Int = var4.next.asInstanceOf[Integer]
            val npoint: DenPoint = points.get(p).asInstanceOf[DenPoint]
            if (!npoint.covered) {
                npoint.covered = true
                mc.insert(npoint, this.timestamp)
                val neighbourhood2: util.ArrayList[Integer] = this.getNeighbourhoodIDs(npoint, this.initBuffer, this.epsilon)
                if (neighbourhood.size > this.minPoints) this.expandCluster(mc, points, neighbourhood2)
            }
        }
    }
    
    private def getNeighbourhoodIDs(point: DenPoint, points: util.ArrayList[DenPoint], eps: Double): util.ArrayList[Integer] = {
        val neighbourIDs: util.ArrayList[Integer] = new util.ArrayList()
        for (p <- 0 until points.size) {
            val npoint: DenPoint = points.get(p).asInstanceOf[DenPoint]
            if (!npoint.covered) {
                val dist: Double = this.distance(point.toDoubleArray, points.get(p).asInstanceOf[DenPoint].toDoubleArray)
                if (dist < eps) neighbourIDs.add(p)
            }
        }
        neighbourIDs
    }
    
    private def nearestCluster(p: DenPoint, cl: Clustering): MicroCluster = {
        var min: MicroCluster = null
        var minDist: Double = 0.0D
        for (c <- 0 until cl.size) {
            val x: MicroCluster = cl.get(c).asInstanceOf[MicroCluster]
            if (min == null) min = x
            var dist: Double = this.distance(p.toDoubleArray, x.getCenter)
            dist -= x.getRadius(this.timestamp)
            if (dist < minDist) {
                minDist = dist
                min = x
            }
        }
        min
    }
    
    private def distance(pointA: Array[Double], pointB: Array[Double]): Double = {
        var distance: Double = 0.0D
        for (i <- 0 until pointA.length) {
            val d: Double = pointA(i) - pointB(i)
            distance += d * d
        }
        Math.sqrt(distance)
    }
    
    override def getClusteringResult: Clustering = {
        val dbscan: DBScan = new DBScan(this.p_micro_cluster, this.offlineOption.getValue * this.epsilon, this.minPoints)
        dbscan.getClustering(this.p_micro_cluster)
    }
    
    override def implementsMicroClusterer: Boolean = true
    
    override def getModelDescription(stringBuilder: lang.StringBuilder, i: Int): Unit = {}
    
    override def getModelMeasurementsImpl: Array[core.Measurement] = {
        throw new UnsupportedOperationException("Not supported yet.")
    }
    
    override def getMicroClusteringResult: Clustering = this.p_micro_cluster
    
    override def isRandomizable: Boolean = true
    
    override def getVotesForInstance(inst: Instance): Array[Double] = null
    
    def getParameterString: String = {
        val sb: StringBuffer = new StringBuffer
        sb.append(this.getClass.getSimpleName + " ")
        sb.append("-" + this.horizonOption.getCLIChar + " ")
        sb.append(this.horizonOption.getValueAsCLIString + " ")
        sb.append("-" + this.epsilonOption.getCLIChar + " ")
        sb.append(this.epsilonOption.getValueAsCLIString + " ")
        sb.append("-" + this.betaOption.getCLIChar + " ")
        sb.append(this.betaOption.getValueAsCLIString + " ")
        sb.append("-" + this.muOption.getCLIChar + " ")
        sb.append(this.muOption.getValueAsCLIString + " ")
        sb.append("-" + this.lambdaOption.getCLIChar + " ")
        sb.append(this.lambdaOption.getValueAsCLIString + " ")
        sb.append("-" + this.initPointsOption.getCLIChar + " ")
        sb.append(this.initPointsOption.getValueAsCLIString)
        sb.toString
    }
    
    override def adjustParameters(): Unit = {
        this.lambda = this.lambdaOption.getValue
        this.epsilon = this.epsilonOption.getValue
        this.minPoints = this.muOption.getValue.toInt
        this.mu = this.muOption.getValue.toInt.toDouble
        this.beta = this.betaOption.getValue
        this.tp = (1.0D / this.lambda * Math.log(this.beta * this.mu / (this.beta * this.mu - 1.0D)).round + 1L).toLong
        this.processingSpeed = this.speedOption.getValue
    }
    
}

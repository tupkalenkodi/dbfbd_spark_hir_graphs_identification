package main

import java.io.File
import scala.collection.mutable.ArrayBuffer


object Utilities {

  // Function to compute neighbor degrees
  private def computeNeighborDegrees(edges: Seq[Seq[Int]], order: Int): Seq[Seq[Int]] = {

    // Initialize adjacency list and degree array
    val adjList = Array.fill(order)(ArrayBuffer.empty[Int])
    val degree = Array.fill(order)(0)

    // Build adjacency list and compute degrees
    edges.foreach {
      case Seq(u, v, _*) =>
        adjList(u) += v
        adjList(v) += u
        degree(u) += 1
        degree(v) += 1
      case _ =>
    }

    // Compute neighbor degrees for each vertex
    (0 until order).map { v =>
      adjList(v).map(u => degree(u)).toSeq
    }
  }

  // Parse edges from string format "a1,b1;a2,b2;..."
   def parseEdges(edgesStr: String): Seq[Seq[Int]] = {
    if (edgesStr == null || edgesStr.isEmpty) {
      return Seq.empty
    }

    edgesStr.split(";").map { edgePair =>
      val parts = edgePair.split(",")
      if (parts.length == 2) {
        Seq(parts(0).toInt, parts(1).toInt)
      } else {
        Seq.empty
      }
    }.filter(_.nonEmpty).toSeq
  }

  // Function to check if graph is highly irregular
  def isHighlyIrregular(edgesStr: String, order: Int): Boolean = {
    // Parse edges from string
    val edges = parseEdges(edgesStr)

    // Trivial cases
    if (edges == null || edges.isEmpty || order <= 0) return false

    // Compute List of Degrees of Neighbors for each vertex
    val neighborDegreesList = computeNeighborDegrees(edges, order)

    // Check if highly irregular
    // For each vertex, all its neighbors must have distinct degrees
    neighborDegreesList.forall { vertexDegrees =>
      // If vertex has neighbors with duplicate degrees, NOT highly irregular
      vertexDegrees.distinct.size == vertexDegrees.size
    }
  }

  // Utility to delete directory recursively
  def deleteDirectory(dir: File): Unit = {
    if (dir.exists()) {
      if (dir.isDirectory) {
        Option(dir.listFiles()).foreach(_.foreach(deleteDirectory))
      }
      dir.delete()
    }
  }
}

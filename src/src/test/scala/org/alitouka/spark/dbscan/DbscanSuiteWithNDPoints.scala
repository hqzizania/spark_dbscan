package org.alitouka.spark.dbscan

class DbscanSuiteWithNDPoints extends DbscanSuiteBase {

  test ("DBSCAN algorithm shouldfind one 3-point cluster and one noise point") {

    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (4)
    val clusteringResult = Dbscan.train (datasetQian, settings)

    val clusters = groupPointsByCluster (clusteringResult)
    clusters.size should be (1)

  }

}


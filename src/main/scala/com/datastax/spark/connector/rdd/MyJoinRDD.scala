package com.datastax.spark.connector.rdd

import com.datastax.driver.core.{ResultSet, ResultSetFuture, Session}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{AllColumns, CassandraRowMetadata, ColumnSelector, PartitionKeyColumns}
import com.datastax.spark.connector.rdd.reader.{PrefetchingResultSetIterator, RowReader, RowReaderFactory}
import com.datastax.spark.connector.writer._
import org.apache.spark.rdd.RDD
import shade.com.datastax.spark.connector.google.common.util.concurrent.{FutureCallback, Futures, SettableFuture}

import scala.collection.immutable
import scala.reflect.ClassTag

/**
  * Created by zulk on 01.06.17.
  */
class MyJoinRDD[L,R] private[connector] (override val left: RDD[L],
                     override val keyspaceName: String,
                     override val tableName: String,
                     override val connector: CassandraConnector,
                     override val columnNames: ColumnSelector = AllColumns,
                     override val joinColumns: ColumnSelector = PartitionKeyColumns,
                     override val where: CqlWhereClause = CqlWhereClause.empty,
                     override val limit: Option[CassandraLimit] = None,
                     override val clusteringOrder: Option[ClusteringOrder] = None,
                     override val readConf: ReadConf = ReadConf(),
                     manualRowReader: Option[RowReader[R]] = None,
                     override val manualRowWriter: Option[RowWriter[L]] = None)
                    (
                      implicit
                      override val leftClassTag: ClassTag[L],
                      override val rightClassTag: ClassTag[R],
                      @transient override val rowWriterFactory: RowWriterFactory[L],
                      @transient override val rowReaderFactory: RowReaderFactory[R])
  extends CassandraJoinRDD[L,R](left,keyspaceName,tableName,connector,columnNames,joinColumns,where,limit,clusteringOrder,readConf,manualRowReader,manualRowWriter){



   private[rdd] def fetchIterator(session: Session, bsb: BoundStatementBuilder[L], leftIterator: Iterator[L]) = {
    val columnNames = selectedColumnRefs.map(_.selectedAs).toIndexedSeq
    val rateLimiter = new RateLimiter(
      readConf.readsPerSec, readConf.readsPerSec
    )

    def pairWithRight(left: L): SettableFuture[Iterator[(L, R)]] = {
      val resultFuture = SettableFuture.create[Iterator[(L, R)]]
      val leftSide = Iterator.continually(left)


      val boundQuery = bsb.bind(left)
      val hash = genHash(boundQuery)
      val queryFuture = checkIfCached(boundQuery)

      Futures.addCallback(queryFuture, new FutureCallback[ResultSet]() {

        def addToCache(rs: ResultSet): Unit = {
          println(s"add to cache $left $hash")
        }

        def onSuccess(rs: ResultSet) {
          addToCache(rs)
          val resultSet = new PrefetchingResultSetIterator(rs, fetchSize)
          val columnMetaData = CassandraRowMetadata.fromResultSet(columnNames, rs);
          val rightSide = resultSet.map(rowReader.read(_, columnMetaData))
          resultFuture.set(leftSide.zip(rightSide))
        }

        def onFailure(throwable: Throwable) {
          resultFuture.setException(throwable)
        }
      })
      resultFuture
    }

    import collection.JavaConversions._
    import com.google.common.hash.Hashing

    def genHash(statement: RichBoundStatement): String = {
      val pc = statement.preparedStatement()
      val allVariables = pc.getVariables
        .asList.map(c => statement.getString(c.getName)).mkString(",")
      val toHash = allVariables+pc.getQueryString
      println(toHash)
      ""
//      Hashing.goodFastHash(64).hashString(toHash).toString
    }

    def checkIfCached(statement: RichBoundStatement): ResultSetFuture = {
      val pc = statement.preparedStatement()
      println(genHash(statement))
      val queryFuture = session.executeAsync(statement)
      queryFuture
    }

    val queryFutures = leftIterator.map(left => {
      rateLimiter.maybeSleep(1)
      pairWithRight(left)
    }).toList
    queryFutures.iterator.flatMap(_.get)
  }
}

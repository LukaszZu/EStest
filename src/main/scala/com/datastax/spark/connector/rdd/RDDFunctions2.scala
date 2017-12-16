package com.datastax.spark.connector.rdd

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.spark.connector.{AllColumns, ColumnSelector, PartitionKeyColumns, RDDFunctions}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by zulk on 01.06.17.
  */
class RDDFunctions2[T](rdd:RDD[T]) extends RDDFunctions(rdd){

  def joinWithCassandraTable2[R](
                                 keyspaceName: String, tableName: String,
                                 selectedColumns: ColumnSelector = AllColumns,
                                 joinColumns: ColumnSelector = PartitionKeyColumns)(
                                 implicit
                                 connector: CassandraConnector = CassandraConnector(sparkContext),
                                 newType: ClassTag[R], rrf: RowReaderFactory[R],
                                 ev: ValidRDDType[R],
                                 currentType: ClassTag[T],
                                 rwf: RowWriterFactory[T]): MyJoinRDD[T, R] = {

    new MyJoinRDD[T, R](
      rdd,
      keyspaceName,
      tableName,
      connector,
      selectedColumns,
      joinColumns
    )
  }

}

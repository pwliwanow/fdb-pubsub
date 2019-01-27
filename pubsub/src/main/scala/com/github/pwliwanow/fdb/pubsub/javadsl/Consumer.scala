package com.github.pwliwanow.fdb.pubsub.javadsl
import java.util.concurrent.CompletableFuture

import akka.NotUsed
import akka.stream.javadsl.{Flow, Sink}
import com.apple.foundationdb.{Database, TransactionContext}
import com.github.pwliwanow.fdb.pubsub.ConsumerRecord
import com.github.pwliwanow.fdb.pubsub.scaladsl.{Consumer => ScalaConsumer}
import com.github.pwliwanow.foundationdb4s.core.DBIO

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContextExecutor

object Consumer {

  def committableFlow[A](
      database: Database,
      ec: ExecutionContextExecutor): Flow[ConsumerRecord[A], ConsumerRecord[A], NotUsed] = {
    ScalaConsumer.committableFlow[A](database)(ec).asJava
  }

  def committableFlow[A, B](
      database: Database,
      f: (TransactionContext, ConsumerRecord[A]) => CompletableFuture[B],
      ec: ExecutionContextExecutor): Flow[ConsumerRecord[A], B, NotUsed] = {
    ScalaConsumer.committableFlow[A, B](database, r => toDBIO(r, f))(ec).asJava
  }

  def committableSink[A](
      database: Database,
      ec: ExecutionContextExecutor): Sink[ConsumerRecord[A], NotUsed] = {
    ScalaConsumer.committableSink[A](database)(ec).asJava
  }

  def committableSink[A, B](
      database: Database,
      f: (TransactionContext, ConsumerRecord[A]) => CompletableFuture[B],
      ec: ExecutionContextExecutor): Sink[ConsumerRecord[A], NotUsed] = {
    ScalaConsumer.committableSink[A, B](database, r => toDBIO(r, f))(ec).asJava
  }

  private def toDBIO[A, B](
      r: ConsumerRecord[A],
      f: (TransactionContext, ConsumerRecord[A]) => CompletableFuture[B]): DBIO[B] = {
    DBIO((tx: TransactionContext, _: ExecutionContextExecutor) => f(tx, r).toScala)
  }

}

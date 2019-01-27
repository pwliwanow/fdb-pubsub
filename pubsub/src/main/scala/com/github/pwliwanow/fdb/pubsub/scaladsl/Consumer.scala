package com.github.pwliwanow.fdb.pubsub.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import com.apple.foundationdb.Database
import com.github.pwliwanow.fdb.pubsub.ConsumerRecord
import com.github.pwliwanow.foundationdb4s.core.DBIO

import scala.concurrent.ExecutionContextExecutor

object Consumer {

  def committableFlow[A](database: Database)(implicit ec: ExecutionContextExecutor)
    : Flow[ConsumerRecord[A], ConsumerRecord[A], NotUsed] = {
    committableFlow[A, ConsumerRecord[A]](database, DBIO.pure)(ec)
  }

  def committableFlow[A, B](database: Database, f: ConsumerRecord[A] => DBIO[B])(
      implicit ec: ExecutionContextExecutor): Flow[ConsumerRecord[A], B, NotUsed] = {
    Flow[ConsumerRecord[A]]
      .mapAsync(1) { record =>
        record.commit
          .flatMap { result =>
            if (result) f(record).map(Some(_))
            else DBIO.pure(None)
          }
          .transact(database)(ec)
      }
      .collect { case Some(x) => x }
  }

  def committableSink[A](database: Database)(
      implicit ec: ExecutionContextExecutor): Sink[ConsumerRecord[A], NotUsed] = {
    committableFlow[A](database).to(Sink.ignore)
  }

  def committableSink[A, B](database: Database, f: ConsumerRecord[A] => DBIO[B])(
      implicit ec: ExecutionContextExecutor): Sink[ConsumerRecord[A], NotUsed] = {
    committableFlow[A, B](database, f).to(Sink.ignore)
  }
}

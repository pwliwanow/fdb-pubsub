package com.github.pwliwanow.fdb.pubsub

import com.github.pwliwanow.foundationdb4s.core.DBIO

final case class ConsumerRecord[A](data: A, partitionNumber: Int, commit: DBIO[Boolean]) {

  def map[B](f: A => B): ConsumerRecord[B] = {
    ConsumerRecord(data = f(data), partitionNumber, commit)
  }
}

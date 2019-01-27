package com.github.pwliwanow.fdb.pubsub

import scala.concurrent.duration._

object ConsumerSettings {
  def apply(): ConsumerSettings = {
    ConsumerSettings(
      acquireLocksInitialDelay = 0.millis,
      acquireLocksInterval = 2.seconds,
      partitionPollingInterval = 100.millis,
      lockValidityDuration = 5.seconds)
  }

  def create(): ConsumerSettings = apply()
}

case class ConsumerSettings(
    acquireLocksInitialDelay: FiniteDuration,
    acquireLocksInterval: FiniteDuration,
    partitionPollingInterval: FiniteDuration,
    lockValidityDuration: FiniteDuration)

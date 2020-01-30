package com.github.pwliwanow.fdb.pubsub

import scala.concurrent.duration._

object ConsumerSettings {
  def apply(): ConsumerSettings = {
    ConsumerSettings(
      acquireLocksInitialDelay = 0.millis,
      acquireLocksInterval = 5.seconds,
      partitionPollingInterval = 1.second,
      lockValidityDuration = 12.seconds)
  }

  def create(): ConsumerSettings = apply()
}

case class ConsumerSettings(
    acquireLocksInitialDelay: FiniteDuration,
    acquireLocksInterval: FiniteDuration,
    partitionPollingInterval: FiniteDuration,
    lockValidityDuration: FiniteDuration)

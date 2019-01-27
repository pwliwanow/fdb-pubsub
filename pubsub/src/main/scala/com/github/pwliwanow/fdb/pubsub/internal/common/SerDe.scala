package com.github.pwliwanow.fdb.pubsub.internal.common

import java.nio.ByteBuffer
import java.time.Instant

private[pubsub] object SerDe {

  def encodeInt(value: Int): Array[Byte] = {
    val output = new Array[Byte](4)
    ByteBuffer.wrap(output).putInt(value)
    output
  }

  def decodeInt(value: Array[Byte]): Int = {
    if (value.length != 4) throw new IllegalArgumentException("Array must be of size 4")
    ByteBuffer.wrap(value).getInt
  }

  def encodeInstant(value: Instant): Long = value.toEpochMilli

  def decodeInstant(value: Long): Instant = Instant.ofEpochMilli(value)

}

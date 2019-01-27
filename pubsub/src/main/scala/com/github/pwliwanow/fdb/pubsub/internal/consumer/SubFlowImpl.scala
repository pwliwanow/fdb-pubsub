package com.github.pwliwanow.fdb.pubsub.internal.consumer
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink, SubFlow}

/** Implementation taken mostly from Akka, where it's a part of an internal API.
  * Turns out, that implementation is also suitable here.
  */
private[pubsub] class SubFlowImpl[In, Out, Mat, F[+ _], C, UMat](
    val subFlow: Flow[In, Out, UMat],
    mergeBackFunction: MergeBack[In, F, UMat],
    finishFunction: Sink[In, UMat] => C)
    extends SubFlow[Out, Mat, F, C] {

  override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): Repr[T] =
    new SubFlowImpl[In, T, Mat, F, C, UMat](subFlow.via(flow), mergeBackFunction, finishFunction)

  override def withAttributes(attr: Attributes): SubFlow[Out, Mat, F, C] =
    new SubFlowImpl[In, Out, Mat, F, C, UMat](
      subFlow.withAttributes(attr),
      mergeBackFunction,
      finishFunction)

  override def addAttributes(attr: Attributes): SubFlow[Out, Mat, F, C] =
    new SubFlowImpl[In, Out, Mat, F, C, UMat](
      subFlow.addAttributes(attr),
      mergeBackFunction,
      finishFunction)

  override def named(name: String): SubFlow[Out, Mat, F, C] =
    new SubFlowImpl[In, Out, Mat, F, C, UMat](
      subFlow.named(name),
      mergeBackFunction,
      finishFunction)

  override def async: Repr[Out] =
    new SubFlowImpl[In, Out, Mat, F, C, UMat](subFlow.async, mergeBackFunction, finishFunction)

  override def mergeSubstreamsWithParallelism(breadth: Int): F[Out] =
    mergeBackFunction(subFlow, breadth)

  override def to[M](sink: Graph[SinkShape[Out], M]): C = {
    finishFunction(subFlow.to(sink))
  }
}

private[pubsub] trait MergeBack[In, F[+ _], UMat] {
  def apply[T](f: Flow[In, T, UMat], breadth: Int): F[T]
}

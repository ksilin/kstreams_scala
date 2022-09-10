package com.example.util

import scala.concurrent.{Future, Promise}
import scala.util.Try

trait FutureConverter {

  implicit class FutureConverter[T](jFuture: java.util.concurrent.Future[T]) {

    def toScalaFuture: Future[T] = {
      val promise = Promise[T]()
      new Thread(() =>
        promise.complete(Try {
          jFuture.get
        })
      ).start()
      promise.future
    }

  }

}

object FutureConverter {

  def toScalaFuture[T](jFuture: java.util.concurrent.Future[T]): Future[T] = {
    val promise = Promise[T]()
    new Thread(() =>
      promise.complete(Try {
        jFuture.get
      })
    ).start()
    promise.future
  }

}

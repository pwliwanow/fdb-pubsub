package com.github.pwliwanow.fdb.pubsub.internal.locking

import java.time.Clock
import java.util.UUID

import cats.instances.all._
import cats.syntax.all._
import com.apple.foundationdb.Database
import com.apple.foundationdb.tuple.Versionstamp
import com.github.pwliwanow.foundationdb4s.core.DBIO

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContextExecutor, Future}

private[pubsub] class ConsumerLockService(
    lockSubspace: ConsumersLockSubspace,
    clock: Clock,
    lockValidityDuration: FiniteDuration) {

  def acquire(key: ConsumerLockKey, acquiredBy: UUID, database: Database)(
      implicit ec: ExecutionContextExecutor): Future[Option[ConsumerLock]] = {
    val now = clock.instant
    def doAcquire(): DBIO[Boolean] =
      lockSubspace.set(ConsumerLock(key, Versionstamp.incomplete(), now, acquiredBy)).map(_ => true)
    val dbio = for {
      maybeCurrentLock <- lockSubspace.get(key).toDBIO
      acquired <- maybeCurrentLock
        .filter(!isExpired(_))
        .fold(doAcquire())(_ => DBIO.pure(false))
    } yield acquired
    dbio.transactVersionstamped(database).map {
      case (acquired, maybeVersionstamp) =>
        maybeVersionstamp
          .filter(_ => acquired)
          .map(ConsumerLock(key, _, now, acquiredBy))
    }
  }

  def forceAcquire(existingLock: ConsumerLock, forceAcquireBy: UUID, database: Database)(
      implicit ec: ExecutionContextExecutor): Future[Option[ConsumerLock]] = {
    val now = clock.instant
    def doAcquire(): DBIO[Boolean] =
      lockSubspace
        .set(ConsumerLock(existingLock.key, Versionstamp.incomplete(), now, forceAcquireBy))
        .map(_ => true)
    val dbio = for {
      maybeCurrentLock <- lockSubspace.get(existingLock.key).toDBIO
      acquired <- maybeCurrentLock
        .filter(_.acquiredWith == existingLock.acquiredWith)
        .fold(DBIO.pure(false))(_ => doAcquire())
    } yield acquired
    dbio.transactVersionstamped(database).map {
      case (acquired, maybeVersionstamp) =>
        maybeVersionstamp
          .filter(_ => acquired)
          .map(ConsumerLock(existingLock.key, _, now, forceAcquireBy))
    }
  }

  def refresh(
      key: ConsumerLockKey,
      acquiredBy: UUID,
      acquiredWith: Versionstamp,
      database: Database)(implicit ec: ExecutionContextExecutor): Future[Option[ConsumerLock]] = {
    val now = clock.instant
    def doRefresh(): DBIO[Unit] = lockSubspace.set(ConsumerLock(key, acquiredWith, now, acquiredBy))
    val dbio: DBIO[Boolean] = for {
      maybeLock <- lockSubspace.get(key).toDBIO
      shouldBeRefreshed = maybeLock.fold(false)(_.acquiredWith == acquiredWith)
      _ <- if (shouldBeRefreshed) doRefresh() else DBIO.unit
    } yield shouldBeRefreshed
    dbio.transact(database).map { refreshed =>
      if (refreshed) Some(ConsumerLock(key, acquiredWith, now, acquiredBy))
      else None
    }
  }

  def refresh(lock: ConsumerLock, database: Database)(
      implicit ec: ExecutionContextExecutor): Future[Option[ConsumerLock]] = {
    refresh(lock.key, lock.acquiredBy, lock.acquiredWith, database)
  }

  def release(lock: ConsumerLock, database: Database)(
      implicit ec: ExecutionContextExecutor): Future[Unit] = {
    releaseLock(lock).transact(database)
  }

  def release(locks: List[ConsumerLock], database: Database)(
      implicit ec: ExecutionContextExecutor): Future[Unit] = {
    locks
      .traverse(releaseLock)
      .transact(database)
      .map(_ => ())
  }

  private def releaseLock(lock: ConsumerLock): DBIO[Unit] = {
    for {
      maybeCurrentLock <- lockSubspace.get(lock.key).toDBIO
      shouldBeReleased = maybeCurrentLock.fold(false)(_.acquiredWith == lock.acquiredWith)
      _ <- if (shouldBeReleased) lockSubspace.clear(lock.key) else DBIO.unit
    } yield ()
  }

  /** Checks if value of a given lease has expired */
  def isExpired(lock: ConsumerLock): Boolean = {
    lock.refreshedAt.isBefore(clock.instant.minusMillis(lockValidityDuration.toMillis))
  }
}

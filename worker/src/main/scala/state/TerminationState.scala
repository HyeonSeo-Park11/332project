package state

import scala.concurrent.Promise
import scala.concurrent.Future
import global.WorkerState
import global.Restorable

class TerminationState extends Serializable with Restorable {
  private var terminated: Boolean = false
  @transient private lazy val terminatePromise: Promise[Unit] = Promise[Unit]()

  def restoreTransient(): Unit = {
    if (terminated) terminatePromise.trySuccess(())
  }
}

object TerminationState {
  def waitForTerminate: Future[Unit] = WorkerState.terminate.terminatePromise.future

  def markTerminated(): Unit =  {
    WorkerState.synchronized{ WorkerState.terminate.terminated = true }
    WorkerState.terminate.terminatePromise.trySuccess(())
  }

  def isTerminated: Boolean = WorkerState.synchronized {
    WorkerState.terminate.terminated
  }
}

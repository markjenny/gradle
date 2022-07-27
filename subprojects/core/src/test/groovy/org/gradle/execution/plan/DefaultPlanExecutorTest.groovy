/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradle.execution.plan

import org.gradle.StartParameter
import org.gradle.api.Action
import org.gradle.initialization.BuildCancellationToken
import org.gradle.internal.concurrent.DefaultParallelismConfiguration
import org.gradle.internal.concurrent.ExecutorFactory
import org.gradle.internal.resources.DefaultResourceLockCoordinationService
import org.gradle.internal.work.WorkerLeaseRegistry
import org.gradle.internal.work.WorkerLeaseService
import spock.lang.Specification

class DefaultPlanExecutorTest extends Specification {
    def workSource = Mock(WorkSource)
    def worker = Mock(Action)
    def executorFactory = Mock(ExecutorFactory)
    def cancellationHandler = Mock(BuildCancellationToken)
    def coordinationService = new DefaultResourceLockCoordinationService() {
        @Override
        protected void doWait() {
        }
    }
    def workerLeaseService = Mock(WorkerLeaseService)
    def workerLease = Mock(WorkerLeaseRegistry.WorkerLease)
    def executor = new DefaultPlanExecutor(new DefaultParallelismConfiguration(false, 1), executorFactory, workerLeaseService, cancellationHandler, coordinationService, Stub(StartParameter))

    def "executes items until no further items remain"() {
        def node1 = Mock(LocalTaskNode)
        def node2 = Mock(LocalTaskNode)

        when:
        def result = executor.process(workSource, worker)

        then:
        result.failures.empty

        1 * workerLeaseService.currentWorkerLease >> workerLease

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> true
        1 * workSource.selectNext() >> WorkSource.Selection.of(node1)

        then:
        1 * worker.execute(node1)

        then:
        1 * workSource.finishedExecuting(node1, null)
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workSource.selectNext() >> WorkSource.Selection.of(node2)
        1 * workSource.executionState() >> WorkSource.State.MaybeWorkReadyToStart

        then:
        1 * worker.execute(node2)

        then:
        1 * workSource.finishedExecuting(node2, null)
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workSource.selectNext() >> WorkSource.Selection.noMoreWorkToStart()

        1 * workerLease.tryLock() >> true
        3 * workSource.allExecutionComplete() >> true
        1 * workSource.collectFailures([])
        0 * workSource._
        0 * workerLease._
    }

    def "item queue can be empty"() {
        when:
        def result = executor.process(workSource, worker)

        then:
        result.failures.empty

        1 * workerLeaseService.currentWorkerLease >> workerLease

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> true
        1 * workSource.selectNext() >> WorkSource.Selection.noMoreWorkToStart()

        1 * workerLease.tryLock() >> true
        3 * workSource.allExecutionComplete() >> true
        1 * workSource.collectFailures([])
        0 * workSource._
        0 * workerLease._
    }

    def "waits until an item is ready for execution"() {
        def node = Mock(LocalTaskNode)

        when:
        def result = executor.process(workSource, worker)

        then:
        result.failures.empty

        1 * workerLeaseService.currentWorkerLease >> workerLease

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> true
        1 * workSource.selectNext() >> WorkSource.Selection.noWorkReadyToStart()

        then:
        1 * workerLease.unlock()
        1 * workSource.executionState() >> WorkSource.State.NoWorkReadyToStart
        1 * workerLease.unlock()
        1 * workSource.executionState() >> WorkSource.State.MaybeWorkReadyToStart
        1 * workerLease.tryLock() >> true

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> true
        1 * workSource.selectNext() >> WorkSource.Selection.of(node)

        then:
        1 * worker.execute(node)

        then:
        1 * workSource.finishedExecuting(node, null)
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workSource.selectNext() >> WorkSource.Selection.noMoreWorkToStart()

        1 * workerLease.tryLock() >> true
        3 * workSource.allExecutionComplete() >> true
        1 * workSource.collectFailures([])
        0 * workSource._
        0 * workerLease._
    }

    def "waits until worker lease is available"() {
        def node = Mock(LocalTaskNode)

        when:
        def result = executor.process(workSource, worker)

        then:
        result.failures.empty

        1 * workerLeaseService.currentWorkerLease >> workerLease

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> false

        then:
        1 * workerLease.unlock()
        1 * workSource.executionState() >> WorkSource.State.MaybeWorkReadyToStart
        1 * workerLease.tryLock() >> false
        1 * workerLease.unlock()
        1 * workSource.executionState() >> WorkSource.State.MaybeWorkReadyToStart
        1 * workerLease.tryLock() >> true

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> true
        1 * workSource.selectNext() >> WorkSource.Selection.of(node)

        then:
        1 * worker.execute(node)

        then:
        1 * workSource.finishedExecuting(node, null)
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workSource.selectNext() >> WorkSource.Selection.noMoreWorkToStart()

        1 * workerLease.tryLock() >> true
        3 * workSource.allExecutionComplete() >> true
        1 * workSource.collectFailures([])
        0 * workSource._
        0 * workerLease._
    }

    def "work item may be stolen by another worker"() {
        when:
        def result = executor.process(workSource, worker)

        then:
        result.failures.empty

        1 * workerLeaseService.currentWorkerLease >> workerLease

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> true
        1 * workSource.selectNext() >> WorkSource.Selection.noWorkReadyToStart()

        then:
        1 * workerLease.unlock()
        1 * workSource.executionState() >> WorkSource.State.NoWorkReadyToStart
        1 * workerLease.unlock()
        1 * workSource.executionState() >> WorkSource.State.MaybeWorkReadyToStart
        1 * workerLease.tryLock() >> true

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> true
        1 * workSource.selectNext() >> WorkSource.Selection.noMoreWorkToStart()

        1 * workerLease.tryLock() >> true
        3 * workSource.allExecutionComplete() >> true
        1 * workSource.collectFailures([])
        0 * workSource._
        0 * workerLease._
    }

    def "execution is canceled when cancellation requested while an item is executing"() {
        def node = Mock(LocalTaskNode)

        when:
        def result = executor.process(workSource, worker)

        then:
        result.failures.empty
        1 * workerLeaseService.currentWorkerLease >> workerLease

        then:
        1 * cancellationHandler.isCancellationRequested() >> false
        1 * workerLease.tryLock() >> true
        1 * workSource.selectNext() >> WorkSource.Selection.of(node)
        1 * worker.execute(node)
        1 * workSource.finishedExecuting(node, null)

        then:
        1 * cancellationHandler.isCancellationRequested() >> true
        1 * workSource.cancelExecution()
        1 * workSource.selectNext() >> WorkSource.Selection.noMoreWorkToStart()

        1 * workerLease.tryLock() >> true
        3 * workSource.allExecutionComplete() >> true
        1 * workSource.collectFailures([])
        0 * workSource._
        0 * workerLease._
    }
}

/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.batch

import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import com.netflix.spinnaker.orca.DefaultTaskResult
import com.netflix.spinnaker.orca.Task
import com.netflix.spinnaker.orca.batch.exceptions.ExceptionHandler
import com.netflix.spinnaker.orca.batch.listeners.SpringBatchExecutionListenerProvider
import com.netflix.spinnaker.orca.batch.listeners.SpringBatchStageListener
import com.netflix.spinnaker.orca.listeners.ExecutionListener
import com.netflix.spinnaker.orca.listeners.StageListener
import com.netflix.spinnaker.orca.listeners.StageStatusPropagationListener
import com.netflix.spinnaker.orca.listeners.StageTaskPropagationListener
import com.netflix.spinnaker.orca.pipeline.ExecutionRunner
import com.netflix.spinnaker.orca.pipeline.ExecutionRunnerSpec
import com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder
import com.netflix.spinnaker.orca.pipeline.TaskNode
import com.netflix.spinnaker.orca.pipeline.model.Execution
import com.netflix.spinnaker.orca.pipeline.model.Pipeline
import com.netflix.spinnaker.orca.pipeline.model.PipelineStage
import com.netflix.spinnaker.orca.pipeline.model.Stage
import com.netflix.spinnaker.orca.pipeline.parallel.WaitForRequisiteCompletionStage
import com.netflix.spinnaker.orca.pipeline.parallel.WaitForRequisiteCompletionTask
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository
import com.netflix.spinnaker.orca.pipeline.util.StageNavigator
import com.netflix.spinnaker.orca.test.batch.BatchTestConfiguration
import org.springframework.batch.core.configuration.JobRegistry
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.retry.backoff.Sleeper
import spock.lang.Shared
import spock.lang.Subject
import spock.lang.Unroll
import static com.netflix.spinnaker.orca.ExecutionStatus.REDIRECT
import static com.netflix.spinnaker.orca.ExecutionStatus.SUCCEEDED
import static com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder.StageDefinitionBuilderSupport.getType
import static com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder.StageDefinitionBuilderSupport.newStage
import static com.netflix.spinnaker.orca.pipeline.TaskNode.TaskDefinition
import static com.netflix.spinnaker.orca.pipeline.TaskNode.TaskGraph
import static com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner.STAGE_AFTER
import static com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner.STAGE_BEFORE
import static org.hamcrest.Matchers.containsInAnyOrder
import static spock.util.matcher.HamcrestSupport.expect

class SpringBatchExecutionRunnerSpec extends ExecutionRunnerSpec {

  @Shared
  def stageNavigator = new StageNavigator(Mock(ApplicationContext))

  def applicationContext = new AnnotationConfigApplicationContext()
  @Autowired JobRegistry jobRegistry
  @Autowired JobBuilderFactory jobs
  @Autowired StepBuilderFactory steps
  @Autowired TaskTaskletAdapter taskTaskletAdapter
  @Autowired(required = false) Collection<Task> tasks = []
  @Autowired(required = false) Collection<TestTask> mockTasks = []

  private Map<Class<? extends Task>, TestTask> getTasksByType() {
    mockTasks.collectEntries { [(it.getClass()): it] }
  }
  @Autowired(required = false) Collection<StageListener> stageListeners = []
  @Autowired(required = false)
  Collection<ExecutionListener> executionListeners = []
  @Autowired JobLauncher jobLauncher
  def executionRepository = Stub(ExecutionRepository)

  private void startContext(
    @ClosureParams(
      value = SimpleType,
      options = "org.springframework.beans.factory.config.ConfigurableListableBeanFactory")
      Closure withBeans) {
    applicationContext.with {
      register(BatchTestConfiguration, TaskTaskletAdapterImpl)
      beanFactory.with {
        registerSingleton("executionRepository", executionRepository)
        registerSingleton("exceptionHandler", Mock(ExceptionHandler))
        registerSingleton("stageBuilderProvider", Stub(StageBuilderProvider))
        registerSingleton("stageNavigator", new StageNavigator(applicationContext))
        registerSingleton("sleeper", Stub(Sleeper))
        registerSingleton("waitForRequisiteCompletionTask", new WaitForRequisiteCompletionTask())
        registerSingleton("stageStatusPropagationListener", new SpringBatchStageListener(executionRepository, new StageStatusPropagationListener()))
        registerSingleton("stageTaskPropagationListener", new SpringBatchStageListener(executionRepository, new StageTaskPropagationListener()))
      }
      withBeans(beanFactory)
      refresh()

      beanFactory.autowireBean(this)
    }
  }

  @Override
  ExecutionRunner create(StageDefinitionBuilder... stageDefBuilders) {
    startContext { beanFactory ->
      beanFactory.registerSingleton("test", new TestTask(delegate: Mock(Task)))
      beanFactory.registerSingleton("preLoop", new PreLoopTask(delegate: Mock(Task)))
      beanFactory.registerSingleton("startLoop", new StartLoopTask(delegate: Mock(Task)))
      beanFactory.registerSingleton("endLoop", new EndLoopTask(delegate: Mock(Task)))
      beanFactory.registerSingleton("postLoop", new PostLoopTask(delegate: Mock(Task)))
    }
    return new SpringBatchExecutionRunner(
      stageDefBuilders.toList() + [new WaitForRequisiteCompletionStage()],
      executionRepository,
      jobLauncher,
      jobRegistry,
      jobs,
      steps,
      taskTaskletAdapter,
      tasks,
      new SpringBatchExecutionListenerProvider(executionRepository, stageListeners, executionListeners)
    )
  }

  @Unroll
  def "creates a batch job and runs it in #description mode"() {
    given:
    execution.stages[0].requisiteStageRefIds = []
    executionRepository.retrievePipeline(execution.id) >> execution

    and:
    def stageDefinitionBuilder = Stub(StageDefinitionBuilder) {
      getType() >> stageType
      buildTaskGraph(_) >> new TaskGraph([new TaskDefinition("test", TestTask)])
    }
    @Subject runner = create(stageDefinitionBuilder)

    when:
    runner.start(execution)

    then:
    1 * mockTasks[0].delegate.execute(_) >> new DefaultTaskResult(SUCCEEDED)

    where:
    parallel | description
    true     | "parallel"
    false    | "linear"

    stageType = "foo"
    execution = Pipeline.builder().withId("1").withStage(stageType).withParallel(parallel).build()
  }

  @Unroll
  def "runs synthetic stages in #description mode"() {
    given:
    execution.stages[0].requisiteStageRefIds = []
    executionRepository.retrievePipeline(execution.id) >> execution

    and:
    def stageDefinitionBuilders = [
      Stub(StageDefinitionBuilder) {
        getType() >> stageType
        buildTaskGraph(_) >> new TaskGraph([new TaskDefinition("test", TestTask)])
        aroundStages(_) >> { Stage<Pipeline> parentStage ->
          [
            newStage(execution, "before_${stageType}_2", "before", [:], parentStage, STAGE_BEFORE),
            newStage(execution, "before_${stageType}_1", "before", [:], parentStage, STAGE_BEFORE),
            newStage(execution, "after_$stageType", "after", [:], parentStage, STAGE_AFTER)
          ]
        }
      },
      Stub(StageDefinitionBuilder) {
        getType() >> "before_${stageType}_1"
        buildTaskGraph(_) >> new TaskGraph([new TaskDefinition("before_test_1", TestTask)])
      },
      Stub(StageDefinitionBuilder) {
        getType() >> "before_${stageType}_2"
        buildTaskGraph(_) >> new TaskGraph([new TaskDefinition("before_test_2", TestTask)])
      },
      Stub(StageDefinitionBuilder) {
        getType() >> "after_$stageType"
        buildTaskGraph(_) >> new TaskGraph([new TaskDefinition("after_test", TestTask)])
      }
    ]
    @Subject runner = create(*stageDefinitionBuilders)

    and:
    def executedStageTypes = []
    mockTasks[0].delegate.execute(_) >> { Stage stage ->
      executedStageTypes << stage.type
      new DefaultTaskResult(SUCCEEDED)
    }

    when:
    runner.start(execution)

    then:
    executedStageTypes == ["before_${stageType}_1", "before_${stageType}_2", stageType, "after_$stageType"]

    where:
    parallel | description
    true     | "parallel"
    false    | "linear"

    stageType = "foo"
    execution = Pipeline.builder().withId("1").withStage(stageType).withParallel(parallel).build()
  }

  // TODO: push up to superclass
  def "executes stage graph in the correct order"() {
    given:
    def startStage = new PipelineStage(execution, "start")
    def branchAStage = new PipelineStage(execution, "branchA")
    def branchBStage = new PipelineStage(execution, "branchB")
    def endStage = new PipelineStage(execution, "end")

    startStage.refId = "1"
    branchAStage.refId = "2"
    branchBStage.refId = "3"
    endStage.refId = "4"

    startStage.requisiteStageRefIds = []
    branchAStage.requisiteStageRefIds = [startStage.refId]
    branchBStage.requisiteStageRefIds = [startStage.refId]
    endStage.requisiteStageRefIds = [branchAStage.refId, branchBStage.refId]

    execution.stages << startStage
    execution.stages << endStage
    execution.stages << branchBStage
    execution.stages << branchAStage

    executionRepository.retrievePipeline(execution.id) >> execution

    and:
    def startStageDefinitionBuilder = stageDefinition(startStage.type) { builder -> builder.withTask("test", TestTask) }
    def branchAStageDefinitionBuilder = Stub(StageDefinitionBuilder) {
      getType() >> branchAStage.type
      buildTaskGraph(_) >> new TaskGraph([new TaskDefinition("test", TestTask)])
    }
    def branchBStageDefinitionBuilder = Stub(StageDefinitionBuilder) {
      getType() >> branchBStage.type
      buildTaskGraph(_) >> new TaskGraph([new TaskDefinition("test", TestTask)])
    }
    def endStageDefinitionBuilder = Stub(StageDefinitionBuilder) {
      getType() >> endStage.type
      buildTaskGraph(_) >> new TaskGraph([new TaskDefinition("test", TestTask)])
    }
    @Subject runner = create(startStageDefinitionBuilder, branchAStageDefinitionBuilder, branchBStageDefinitionBuilder, endStageDefinitionBuilder)

    and:
    def executedStageTypes = []
    mockTasks[0].delegate.execute(_) >> { Stage stage ->
      executedStageTypes << stage.type
      new DefaultTaskResult(SUCCEEDED)
    }

    when:
    runner.start(execution)

    then:
    expect executedStageTypes, containsInAnyOrder(startStage.type, branchAStage.type, branchBStage.type, endStage.type)
    executedStageTypes.first() == startStage.type
    executedStageTypes.last() == endStage.type

    where:
    execution = Pipeline.builder().withId("1").withParallel(true).build()
  }

  def "executes loops"() {
    given:
    def stage = new PipelineStage(execution, "looping")
    execution.stages << stage

    executionRepository.retrievePipeline(execution.id) >> execution

    and:
    def stageDefinitionBuilder = stageDefinition("looping") { builder ->
      builder
        .withTask("preLoop", PreLoopTask)
        .withLoop({ subGraph ->
        subGraph
          .withTask("startLoop", StartLoopTask)
          .withTask("endLoop", EndLoopTask)
      })
        .withTask("postLoop", PostLoopTask)
    }
    @Subject runner = create(stageDefinitionBuilder)

    when:
    runner.start(execution)

    then:
    // TODO: make this a field
    1 * tasksByType[PreLoopTask].delegate.execute(_) >> new DefaultTaskResult(SUCCEEDED)
    3 * tasksByType[StartLoopTask].delegate.execute(_) >> new DefaultTaskResult(SUCCEEDED)
    3 * tasksByType[EndLoopTask].delegate.execute(_) >> new DefaultTaskResult(REDIRECT) >> new DefaultTaskResult(REDIRECT) >> new DefaultTaskResult(SUCCEEDED)
    1 * tasksByType[PostLoopTask].delegate.execute(_) >> new DefaultTaskResult(SUCCEEDED)

    where:
    execution = Pipeline.builder().withId("1").withParallel(true).build()
  }

  @CompileStatic
  static class TestTask implements Task {
    @Delegate
    Task delegate
  }

  @CompileStatic
  static class PreLoopTask extends TestTask {}

  @CompileStatic
  static class StartLoopTask extends TestTask {}

  @CompileStatic
  static class EndLoopTask extends TestTask {}

  @CompileStatic
  static class PostLoopTask extends TestTask {}

  @CompileStatic
  static StageDefinitionBuilder stageDefinition(
    String name,
    @ClosureParams(
      value = SimpleType,
      options = "com.netflix.spinnaker.orca.pipeline.TaskNode.Builder"
    )
      Closure<Void> closure) {
    return new StageDefinitionBuilder() {
      @Override
      public <T extends Execution<T>> void taskGraph(Stage<T> parentStage, TaskNode.Builder builder) {
        closure(builder)
      }

      @Override
      String getType() {
        name
      }
    }
  }

}

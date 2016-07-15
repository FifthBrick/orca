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
import org.spockframework.spring.xml.SpockMockFactoryBean
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.support.GenericApplicationContext
import org.springframework.retry.backoff.Sleeper
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
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
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD
import static spock.util.matcher.HamcrestSupport.expect

@ContextConfiguration(classes = [
  BatchTestConfiguration, TaskTaskletAdapterImpl, StageNavigator,
  WaitForRequisiteCompletionTask, SpringBatchExecutionListenerProvider,
  MockTaskConfiguration, WaitForRequisiteCompletionStage
])
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
class SpringBatchExecutionRunnerSpec extends ExecutionRunnerSpec {

  @Autowired GenericApplicationContext applicationContext
  @Autowired TestTask testTask
  @Autowired PreLoopTask preLoopTask
  @Autowired StartLoopTask startLoopTask
  @Autowired EndLoopTask endLoopTask
  @Autowired PostLoopTask postLoopTask

  @Autowired ExecutionRepository executionRepository

  def setup() {
    println "appcontext=" + System.identityHashCode(applicationContext)
  }

  @Override
  ExecutionRunner create(StageDefinitionBuilder... stageDefBuilders) {
    applicationContext.with {
      stageDefBuilders.each {
        beanFactory.registerSingleton(it.type, it)
      }
      autowireCapableBeanFactory.createBean(SpringBatchExecutionRunner)
    }
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
    1 * testTask.execute(_) >> new DefaultTaskResult(SUCCEEDED)

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
    testTask.execute(_) >> { Stage stage ->
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
    testTask.execute(_) >> { Stage stage ->
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
    1 * preLoopTask.execute(_) >> new DefaultTaskResult(SUCCEEDED)
    3 * startLoopTask.execute(_) >> new DefaultTaskResult(SUCCEEDED)
    3 * endLoopTask.execute(_) >> new DefaultTaskResult(REDIRECT) >> new DefaultTaskResult(REDIRECT) >> new DefaultTaskResult(SUCCEEDED)
    1 * postLoopTask.execute(_) >> new DefaultTaskResult(SUCCEEDED)

    where:
    execution = Pipeline.builder().withId("1").withParallel(true).build()
  }

  @CompileStatic
  static interface TestTask extends Task {}

  @CompileStatic
  static interface PreLoopTask extends Task {}

  @CompileStatic
  static interface StartLoopTask extends Task {}

  @CompileStatic
  static interface EndLoopTask extends Task {}

  @CompileStatic
  static interface PostLoopTask extends Task {}

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

  @Configuration
  @CompileStatic
  static class MockTaskConfiguration {
    @Bean
    FactoryBean<ExecutionRepository> executionRepository() {
      new SpockMockFactoryBean(ExecutionRepository)
    }

    @Bean
    FactoryBean<ExceptionHandler> exceptionHandler() {
      new SpockMockFactoryBean(ExceptionHandler)
    }

    @Bean
    FactoryBean<StageBuilderProvider> builderProvider() {
      new SpockMockFactoryBean(StageBuilderProvider)
    }

    @Bean
    FactoryBean<Sleeper> sleeper() { new SpockMockFactoryBean(Sleeper) }

    @Bean
    StageListener stageStatusPropagationListener(ExecutionRepository executionRepository) {
      new SpringBatchStageListener(executionRepository, new StageStatusPropagationListener())
    }

    @Bean
    StageListener stageTaskPropagationListener(ExecutionRepository executionRepository) {
      new SpringBatchStageListener(executionRepository, new StageTaskPropagationListener())
    }

    @Bean
    FactoryBean<TestTask> testTask() { new SpockMockFactoryBean(TestTask) }

    @Bean
    FactoryBean<PreLoopTask> preLoopTask() {
      new SpockMockFactoryBean(PreLoopTask)
    }

    @Bean
    FactoryBean<StartLoopTask> startLoopTask() {
      new SpockMockFactoryBean(StartLoopTask)
    }

    @Bean
    FactoryBean<EndLoopTask> endLoopTask() {
      new SpockMockFactoryBean(EndLoopTask)
    }

    @Bean
    FactoryBean<PostLoopTask> postLoopTask() {
      new SpockMockFactoryBean(PostLoopTask)
    }
  }
}

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

package com.netflix.spinnaker.orca.pipeline;

import java.util.*;
import java.util.stream.Collectors;
import com.netflix.spinnaker.orca.pipeline.TaskNode.TaskDefinition;
import com.netflix.spinnaker.orca.pipeline.model.DefaultTask;
import com.netflix.spinnaker.orca.pipeline.model.Execution;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner;
import static com.google.common.collect.Lists.reverse;
import static com.netflix.spinnaker.orca.ExecutionStatus.NOT_STARTED;
import static com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner.STAGE_AFTER;
import static com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner.STAGE_BEFORE;
import static java.lang.String.format;

public abstract class ExecutionRunnerSupport implements ExecutionRunner {
  private final Collection<StageDefinitionBuilder> stageDefinitionBuilders;

  public ExecutionRunnerSupport(Collection<StageDefinitionBuilder> stageDefinitionBuilders) {
    this.stageDefinitionBuilders = stageDefinitionBuilders;
  }

  /**
   * The default implementation plans all stages.
   * Implementations may override this method completely or use it to plan the entire execution before running it.
   *
   * @param execution the execution instance with skeleton stages.
   * @param <T>       the execution type.
   * @throws Exception
   */
  @Override
  public <T extends Execution<T>> void start(T execution) throws Exception {
    List<Stage<T>> stages = new ArrayList<>(execution.getStages()); // need to clone because we'll be modifying the list
    stages.stream().forEach(this::planStage);
  }

  /**
   * Plans the tasks in a stage including any pre and post stages.
   * Implementations may call this directly before executing an individual stage or all in advance.
   *
   * @param stage the stage with no tasks currently attached.
   * @param <T>   the execution type.
   */
  protected <T extends Execution<T>> void planStage(Stage<T> stage) {
    StageDefinitionBuilder builder = findBuilderForStage(stage);
    Map<SyntheticStageOwner, List<Stage<T>>> allStages = builder
      .aroundStages(stage)
      .stream()
      .collect(Collectors.groupingBy(Stage::getSyntheticStageOwner));

    reverse(
      allStages.getOrDefault(STAGE_BEFORE, Collections.emptyList())
    ).forEach(preStage -> {
      List<Stage<T>> stages = stage.getExecution().getStages();
      int index = stages.indexOf(stage);
      stages.add(index, preStage);
      preStage.setParentStageId(stage.getId());
      planStage(preStage);
    });

    reverse(
      allStages.getOrDefault(STAGE_AFTER, Collections.emptyList())
    ).forEach(postStage -> {
      List<Stage<T>> stages = stage.getExecution().getStages();
      int index = stages.indexOf(stage);
      stages.add(index + 1, postStage);
      postStage.setParentStageId(stage.getId());
      planStage(postStage);
    });

    TaskNode.TaskGraph taskGraph = builder.buildTaskGraph(stage);
    planTasks(stage, taskGraph, false);
  }

  private <T extends Execution<T>> void planTasks(Stage<T> stage, TaskNode.TaskGraph taskGraph, boolean isSubGraph) {
    for (ListIterator<TaskNode> itr = taskGraph.listIterator(); itr.hasNext(); ) {
      boolean isStart = !itr.hasPrevious();
      // do this after calling itr.hasPrevious because ListIterator is stupid
      TaskNode taskDef = itr.next();
      boolean isEnd = !itr.hasNext();

      if (taskDef instanceof TaskDefinition) {
        DefaultTask task = new DefaultTask();
        if (isStart) {
          if (isSubGraph) {
            task.setLoopStart(true);
          } else {
            task.setStageStart(true);
          }
        }
        task.setId(String.valueOf((stage.getTasks().size() + 1)));
        task.setName(((TaskDefinition) taskDef).getName());
        task.setStatus(NOT_STARTED);
        task.setImplementingClass(((TaskDefinition) taskDef).getImplementingClass());
        if (isEnd) {
          if (isSubGraph) {
            task.setLoopEnd(true);
          } else {
            // TODO: may need to revisit this for parallel stages like bake & deploy
            task.setStageEnd(true);
          }
        }
        stage.getTasks().add(task);
      } else if (taskDef instanceof TaskNode.TaskGraph) {
        planTasks(stage, (TaskNode.TaskGraph) taskDef, true);
      } else {
        throw new UnsupportedOperationException(format("Unknown TaskNode type %s", taskDef.getClass().getName()));
      }
    }
  }

  private <T extends Execution<T>> StageDefinitionBuilder findBuilderForStage(Stage<T> stage) {
    return stageDefinitionBuilders
      .stream()
      .filter(builder -> builder.getType().equals(stage.getType()))
      .findFirst()
      .orElseThrow(() -> new NoSuchStageDefinitionBuilder(stage.getType()));
  }
}

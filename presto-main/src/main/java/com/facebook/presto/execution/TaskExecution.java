/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.TaskSource;
import com.facebook.presto.operator.TaskContext;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.List;

public interface TaskExecution
{
    TaskId getTaskId();

    TaskInfo getTaskInfo();

    TaskContext getTaskContext();

    void waitForStateChange(TaskState currentState, Duration maxWait)
            throws InterruptedException;

    void addSources(List<TaskSource> sources);

    void addResultQueue(OutputBuffers outputIds);

    void cancel();

    void fail(Throwable cause);

    BufferResult getResults(String outputId, long startingSequenceId, DataSize maxSize, Duration maxWait)
            throws InterruptedException;

    void abortResults(String outputId);

    void recordHeartbeat();
}

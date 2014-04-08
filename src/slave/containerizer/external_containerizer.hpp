/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __EXTERNAL_CONTAINERIZER_HPP__
#define __EXTERNAL_CONTAINERIZER_HPP__

#include <list>
#include <sstream>
#include <string>

#include <process/owned.hpp>
#include <process/subprocess.hpp>

#include <stout/hashmap.hpp>
#include <stout/try.hpp>
#include <stout/tuple.hpp>

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/isolator.hpp"
#include "slave/containerizer/launcher.hpp"

namespace mesos {
namespace internal {
namespace slave {

// The scheme an external containerizer has to adhere to is;
//
// COMMAND (ADDITIONAL-PARAMETERS) < INPUT-PROTO > RESULT-PROTO
//
// launch (ContainerID) < ExternalTask > ExternalStatus
// update (ContainerID) < ResourceArray > ExternalStatus
// usage (ContainerID) > ResourceStatistics
// wait (ContainerID) > Termination
// destroy (ContainerID) > ExternalStatus
//
// NOTE: ExternalStatus is currently used for synchronizing and human
// readable logging. The embedded message does not have to adhere to
// any scheme but must not be empty for valid results.

// For debugging purposes of an external containerizer, it might be
// helpful to enable verbose logging on the slave (GLOG_v=2).

class ExternalContainerizerProcess;

class ExternalContainerizer : public Containerizer
{
public:
  ExternalContainerizer(const Flags& flags);

  virtual ~ExternalContainerizer();

  virtual process::Future<Nothing> recover(
      const Option<state::SlaveState>& state);

  virtual process::Future<ExecutorInfo> launch(
      const ContainerID& containerId,
      const TaskInfo& task,
      const FrameworkID& frameworkId,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual void destroy(const ContainerID& containerId);

private:
  ExternalContainerizerProcess* process;
};


class ExternalContainerizerProcess
  : public process::Process<ExternalContainerizerProcess>
{
public:
  ExternalContainerizerProcess(const Flags& flags);

  // Recover containerized executors as specified by state. See
  // containerizer.hpp:recover for more.
  process::Future<Nothing> recover(const Option<state::SlaveState>& state);

  // Start the containerized executor.
  process::Future<ExecutorInfo> launch(
      const ContainerID& containerId,
      const TaskInfo& task,
      const FrameworkID& frameworkId,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  // Update the container's resources.
  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  // Gather resource usage statistics for the containerized executor.
  process::Future<ResourceStatistics> usage(const ContainerID& containerId);

  // Get a future on the containerized executor's Termination.
  process::Future<containerizer::Termination> wait(
      const ContainerID& containerId);

  // Terminate the containerized executor.
  void destroy(const ContainerID& containerId);

private:
  // Startup flags.
  const Flags flags;

  // Wraps futures of both, the result (protobuf-message) and the
  // command's exit-code of the external containerizer. A tuple is
  // used, so it can directly feed into the process::await-tuple-
  // overload.
  typedef tuples::tuple<
      process::Future<std::string>,
      process::Future<Option<int> > > ResultFutures;

  // Information describing a running container.
  struct Container
  {
    Container() {}

    // External containerizer pid as per wait-invocation.
    pid_t pid;

    process::Promise<containerizer::Termination> termination;
    process::Promise<bool> waited;

    Resources resources;
  };

  // Stores all launched processes.
  hashmap<ContainerID, process::Owned<Container> > containers;

  // Information describing a container environment. A sandbox has to
  // be prepared before the external containerizer can be invoked.
  // As the Container struct is populated after the 'launch'
  // invocation, bundling this information with the above would
  // trigger additionl state check overhead.
  struct Sandbox
  {
    Sandbox(const std::string& directory, const Option<std::string>& user)
      : directory(directory), user(user) {}

    const std::string directory;
    const Option<std::string> user;
  };

  // Stores sandbox specific information.
  hashmap<ContainerID, process::Owned<Sandbox> > sandboxes;

  process::Future<ExecutorInfo> _launch(
      const ContainerID& containerId,
      const FrameworkID& frameworkId,
      const ExecutorInfo executorInfo,
      const SlaveID& slaveId,
      bool checkpoint,
      const process::Future<Option<int> >& future);

  void _wait(
      const ContainerID& containerId,
      const process::Future<Option<int> >& future);

  process::Future<Nothing> _update(
      const ContainerID& containerId,
      const process::Future<Option<int> >& future);

  process::Future<ResourceStatistics> _usage(
      const ContainerID& containerId,
      const process::Future<ResultFutures>& future);

  void _destroy(
      const ContainerID& containerId,
      const process::Future<Option<int> >& future);

  // TODO(tillt): FIXME!
  Try<std::string> isDone(
      const process::Future<ResultFutures>& future);

  Try<Nothing> isDone(
      const process::Future<Option<int> >& future);

  // Sets the pipe in non-blocking mode and reads a size prefixed
  // chunk of data into a string.
  process::Future<std::string> read(int pipe);

  // Terminate a containerizer process and its children, sends a
  // SIGKILL.
  // TODO(tillt): Add graceful termination as soon as we have
  // an accepted way to do that in place.
  void terminate(const ContainerID& containerId);

  // Call back for when the containerizer has terminated all processes
  // in the container.
  void cleanup(const ContainerID& containerId);

  Try<process::Subprocess> invoke(
      const std::string& command,
      const ContainerID& containerId,
      const google::protobuf::Message& message,
      const std::map<std::string, std::string>& environment
        = std::map<std::string, std::string>());
};


// Get an ExecutorInfo that is specific to a container.
ExecutorInfo containerExecutorInfo(
    const Flags& flags,
    const TaskInfo& task,
    const FrameworkID& frameworkId);

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __EXTERNAL_CONTAINERIZER_HPP__

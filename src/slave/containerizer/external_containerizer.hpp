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
// launch (ContainerID, --mesos-executor, <path>) < TaskInfo > ExternalStatus
// update (ContainerID) < ResourceArray > ExternalStatus
// usage (ContainerID) > ResourceStatistics
// wait (ContainerID) > ExternalTermination
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

  virtual process::Future<Containerizer::Termination> wait(
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

  // Recover containerized executors as specified by state. Any
  // containerized executors present on the system but not included
  // in state will be terminated and cleaned up.
  process::Future<Nothing> recover(
      const Option<state::SlaveState>& state);

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

  // Update the container's resources. The executor should not assume
  // the resources have been changed until this method returns.
  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  // Gather resource usage statistics for the containerized executor.
  process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  // Get a future on the containerized executor's Termination.
  process::Future<Containerizer::Termination> wait(
      const ContainerID& containerId);

  // Terminate the containerized executor.
  void destroy(const ContainerID& containerId);

private:
  // Startup flags.
  const Flags flags;

  // Wraps futures of both, the result (protobuf-message) and the
  // command's exit-code of the external containerizer.
  typedef tuples::tuple<
      process::Future<std::string>,
      process::Future<Option<int> > > ResultFutures;

  // Information describing a running container process.
  struct Running
  {
    Running(pid_t _pid) : pid(_pid) {}

    const pid_t pid;
    process::Promise<Containerizer::Termination> termination;
    process::Promise<bool> waited;
    Resources resources;
  };

  // Stores all launched processes.
  hashmap<ContainerID, process::Owned<Running> > running;

  // Information describing a container environment.
  struct Sandbox
  {
    Sandbox(const std::string& directory, const Option<std::string>& user) :
      directory(directory), user(user) {};

    const std::string directory;
    const Option<std::string> user;
  };

  // Stores sandbox specific information.
  hashmap<ContainerID, process::Owned<Sandbox> > sandboxes;

  process::Future<ExecutorInfo> _launch(
      const ContainerID& containerId,
      pid_t pid,
      const FrameworkID& frameworkId,
      const ExecutorInfo executorInfo,
      const SlaveID& slaveId,
      bool checkpoint,
      const process::Future<std::string>& future);

  void _wait(
      const ContainerID& containerId,
      const process::Future<ResultFutures>& future);

  process::Future<Nothing> _update(
      const ContainerID& containerId,
      const process::Future<ResultFutures>& future);

  process::Future<ResourceStatistics> _usage(
      const ContainerID& containerId,
      const process::Future<ResultFutures>& future);

  void _destroy(
      const ContainerID& containerId,
      const process::Future<ResultFutures>& future);

  // Call back for when the pluggable containerizer process status has
  // changed.
  void reaped(const ContainerID& containerId,
      const process::Future<Option<int> >& status);

  // If the exit code is 0 and the data sent on stdout is empty,
  // it is taken to mean that the external containerizer is
  // requesting Mesos use the default strategy for a particular
  // method (usage, wait, and all the rest). Should the external
  // containerizer exit non-zero, it is always an error.
  Try<bool> commandSupported(
      const std::string& input,
      int exitCode = 0);

  Try<bool> commandSupported(
      const process::Future<ResultFutures>& future,
      std::string& result);

  // Try to get the piped protobuf-string which poses as a result
  // of the external, pluggable containerizer out of the ResultFutures.
  Try<std::string> result(const ResultFutures& futures);

  // Try to get the reaped exit-code out of the ResultFutures.
  Try<int> status(const ResultFutures& futures);

  // Sets the pipe in non-blocking mode and reads until the write end
  // of that pipe got closed (EOF).
  process::Future<std::string> read(int pipe);

  // Terminate a containerizer process and its children, sends an
  // initial SIGTERM. After a grace period a SIGKILL is sent to left-
  // over processess.
  void terminate(const ContainerID& containerId);

  // Call back for when the containerizer has terminated all processes
  // in the container.
  void cleanup(const ContainerID& containerId);

  // Call the external, pluggable containerizer and open a pipe for
  // receiving results from that command.
  Try<pid_t> invoke(
      const std::string& command,
      const ContainerID& containerId,
      int& resultPipe);

  Try<pid_t> invoke(
      const std::string& command,
      const ContainerID& containerId,
      const std::string& output,
      int& resultPipe);

  Try<pid_t> invoke(
      const std::string& command,
      const std::vector<std::string>& parameters,
      const std::map<std::string, std::string>& environment,
      const ContainerID& containerId,
      const std::string& output,
      int& resultPipe);
};

// Get an ExecutorInfo that is specific to a container.
ExecutorInfo containerExecutorInfo(
    const Flags& flags,
    const TaskInfo& task,
    const FrameworkID& frameworkId);

// Get a human readable error string from an initializing error of a
// message.
std::string initializationError(const google::protobuf::Message& message);

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __EXTERNAL_CONTAINERIZER_HPP__

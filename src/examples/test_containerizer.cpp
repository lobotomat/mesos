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

// The scheme an external containerizer has to adhere to is;
//
// COMMAND < INPUT-PROTO > RESULT-PROTO
//
// launch < containerizer::Launch
// update < containerizer::Update
// usage < containerizer::Usage > ResourceStatistics
// wait < containerizer::Wait > containerizer::Termination
// destroy < containerizer::Destroy
// containers > containerizer::Containers
// recover
//
// 'wait' is expected to block until the task command/executor has
// terminated.

#include <stdlib.h>     // exit

#include <iostream>
#include <fstream>
#include <string>

#include <mesos/mesos.hpp>
#include <mesos/mesos.hpp>
#include <mesos/containerizer/containerizer.hpp>

#include <process/owned.hpp>
#include <process/pid.hpp>
#include <process/dispatch.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include <stout/hashset.hpp>
#include <stout/flags.hpp>
#include <stout/lambda.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>

#include "messages/messages.hpp"

#include "slave/flags.hpp"

#include "slave/containerizer/mesos_containerizer.hpp"

#include "slave/containerizer/isolators/posix.hpp"

#include "examples/test_containerizer.pb.h"

using namespace mesos;
using namespace mesos::containerizer;
using namespace mesos::containerizer::examples;

using process::Owned;
using process::UPID;
using process::PID;
using process::Future;

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::vector;

using lambda::function;


template<typename T>
Result<T> receive()
{
  return ::protobuf::read<T>(STDIN_FILENO, false, false);
}


Try<Nothing> send(const google::protobuf::Message& message)
{
  return ::protobuf::write(STDOUT_FILENO, message);
}


template<typename T>
static Try<T> validate(const Future<T>& future)
{
  if (!future.isReady()) {
    return Error("Outer result "
        + (future.isFailed() ? "failed: " + future.failure()
                             : "got discarded"));
  }

  T result = future.get();
  if (result.future().status() != examples::FUTURE_READY) {
    return Error("Inner result "
        + (result.future().status() ==  examples::FUTURE_FAILED
            ? "failed: " + future.failure()
            : "got discarded"));
  }

  return result;
}


namespace mesos {
namespace internal {
namespace slave {


class TestContainerizerProcess : public MesosContainerizerProcess
{
public:
  TestContainerizerProcess(
      const Flags& flags,
      const Owned<Launcher>& launcher,
      const vector<Owned<Isolator> >& isolators)
    : MesosContainerizerProcess(flags, true, launcher, isolators)
  {
  }

  virtual ~TestContainerizerProcess() {}

private:
};


class ReceiveProcess : public ProtobufProcess<ReceiveProcess>
{
public:
  ReceiveProcess(TestContainerizerProcess* target) : target(target) {}

  void initialize()
  {
    install<ShutdownMessage>(
        &ReceiveProcess::shutdown);

    void (ReceiveProcess::*launch)(
        const UPID&,
        const ContainerID&,
        const TaskInfo&,
        const ExecutorInfo&,
        const string&,
        const Option<string>&,
        const SlaveID&,
        const PID<Slave>&,
        bool checkpoint) = &ReceiveProcess::launch;

    install<Launch>(
        launch,
        &Launch::container_id,
        &Launch::task_info,
        &Launch::executor_info,
        &Launch::directory,
        &Launch::user,
        &Launch::slave_id,
        &Launch::slave_pid,
        &Launch::checkpoint);

    install<Update>(
        &ReceiveProcess::update,
        &Usage::container_id
        &Usage::resources);

    install<Destroy>(
        &ReceiveProcess::destroy,
        &containerizer::Destroy::container_id);

    install<Recover>(
        &ReceiveProcess::recover);

    install<ContainersRequest>(
        &ReceiveProcess::containers);

    install<Wait>(
        &ReceiveProcess::wait,
        &Wait::container_id);

    install<Usage>(
        &ReceiveProcess::usage,
        &Usage::container_id);
  }

  // Future<hashset<ContainerID> > overload
  void reply(const UPID& from, const Future<hashset<ContainerID> >& future)
  {
    FutureMessage message;

    message.set_status(future.isReady()
        ? FUTURE_READY
        : future.isFailed() ? FUTURE_FAILED
                            : FUTURE_DISCARDED);
    if (future.isFailed()) {
      message.set_message(future.failure());
    }

    ContainersResult r;
    r.mutable_future()->CopyFrom(message);

    if (future.isReady()) {
      Containers *result = r.mutable_result();
      foreach(const ContainerID& containerId, future.get()) {
        result->add_containers()->CopyFrom(containerId);
      }
    }

    send(from, r);
  }

  // Future<Nothing> overload
  template<typename R>
  void reply(const UPID& from, const Future<Nothing>& future)
  {
    FutureMessage message;

    message.set_status(future.isReady()
        ? FUTURE_READY
        : future.isFailed() ? FUTURE_FAILED
                            : FUTURE_DISCARDED);
    if (future.isFailed()) {
      message.set_message(future.failure());
    }

    R r;
    r.mutable_future()->CopyFrom(message);

    send(from, r);
  }

  template<typename T, typename R>
  void reply(const UPID& from, const Future<T>& future)
  {
    FutureMessage message;

    message.set_status(future.isReady()
        ? FUTURE_READY
        : future.isFailed() ? FUTURE_FAILED
                            : FUTURE_DISCARDED);
    if (future.isFailed()) {
      message.set_message(future.failure());
    }

    R r;
    r.mutable_future()->CopyFrom(message);
    if (future.isReady()) {
      r.mutable_result()->CopyFrom(future.get());
    }

    send(from, r);
  }

  void launch(
      const UPID& from,
      const ContainerID& containerId,
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const string& directory,
      const string& user,
      const SlaveID& slaveId,
      const string& slavePid,
      bool checkpoint)
  {
    dispatch(
        target,
        &TestContainerizerProcess::launch,
        containerId,
        taskInfo,
        executorInfo,
        directory,
        user,
        slaveId,
        slavePid,
        checkpoint)
      .onAny(defer(
        self(),
        &ReceiveProcess::reply<LaunchResult>,
        from,
        lambda::_1));
  }

  void containers(const UPID& from)
  {
    void (ReceiveProcess::*reply)(
        const UPID&,
        const Future<hashset<ContainerID> >&) = &ReceiveProcess::reply;

    dispatch(
        target,
        &TestContainerizerProcess::containers)
      .onAny(lambda::bind(
          reply,
          this,
          from,
          lambda::_1));
  }

  void wait(const UPID& from, const ContainerID& containerId)
  {
    dispatch(
        target,
        &TestContainerizerProcess::wait,
        containerId)
      .onAny(lambda::bind(
          &ReceiveProcess::reply<Termination, WaitResult>,
          this,
          from,
          lambda::_1));
  }

  void usage(const UPID& from, const ContainerID& containerId)
  {
    dispatch(
        target,
        &TestContainerizerProcess::usage,
        containerId)
      .onAny(lambda::bind(
          &ReceiveProcess::reply<ResourceStatistics, UsageResult>,
          this,
          from,
          lambda::_1));
  }

  void shutdown()
  {
    cerr << "Received shutdown" << endl;
    terminate(target);
  }

private:
  TestContainerizerProcess* target;
};


class TestContainerizer
{
public:
  TestContainerizer() : path("/tmp/mesos-test-containerizer") {}
  virtual ~TestContainerizer() {}

  Option<Error> initialize()
  {
    try {
      std::ifstream file(path::join(path, "pid"));
      file >> pid;
      file.close();
    } catch(std::exception e) {
      return Error(string("Failed reading PID: ") + e.what());
    }
    return None();
  }

  // T=containerizer::Usage
  // R=examples::UsageResult
  template <typename T, typename R>
  Option<Error> thunk(bool piped = true)
  {
    Option<Error> init = initialize();
    if (init.isSome()) {
      return Error("Failed to initialize: " + init.get().message);
    }

    T t;
    // Receive the message via pipe, if needed.
    if (piped) {
      Result<T> result = receive<T>();
      if (result.isError()) {
        return Error("Failed to receive from pipe: " + result.error());
      }
      t.CopyFrom(result.get());
    }

    // Send a request and receive an answer via process protobuf
    // exchange.
    struct Protocol<T, R> protocol;
    Future<R> future = protocol(pid, t);

    future.await();

    Try<R> r = validate(future);
    if (r.isError()) {
      return Error("Exchange failed: " + r.error());
    }

    if (r.get().has_result()) {
      // Send the payload message via pipe.
      Try<Nothing> sent = send(r.get().result());
      if (sent.isError()) {
        return Error("Failed to send to pipe: " + sent.error());
      }
    }

    return None();
  }

  int setup()
  {
    Flags flags;

    // Create a MesosContainerizerProcess using isolators and a launcher.
    vector<Owned<Isolator> > isolators;

    Try<Isolator*> cpuIsolator = PosixCpuIsolatorProcess::create(flags);
    if (cpuIsolator.isError()) {
      cerr << "Could not create PosixCpuIsolator: " << cpuIsolator.error()
           << endl;
      return 1;
    }
    isolators.push_back(Owned<Isolator>(cpuIsolator.get()));

    Try<Isolator*> memIsolator = PosixMemIsolatorProcess::create(flags);
    if (memIsolator.isError()) {
      cerr << "Could not create PosixMemIsolator: " << memIsolator.error()
           << endl;
      return 1;
    }
    isolators.push_back(Owned<Isolator>(memIsolator.get()));

    Try<Launcher*> launcher = PosixLauncher::create(flags);
    if (launcher.isError()) {
      cerr << "Could not create PosixLauncher: " << launcher.error() << endl;
      return 1;
    }

    TestContainerizerProcess* process = new TestContainerizerProcess(
        flags, Owned<Launcher>(launcher.get()), isolators);

    spawn(process, false);

    ReceiveProcess* receive = new ReceiveProcess(process);
    spawn(receive, false);

    pid = PID<ReceiveProcess>(receive);

    // Serialize the UPID.
    std::ofstream file(path::join(path, "pid"));
    file << pid;
    file.close();

    cerr << "PID: " << pid << endl;

    process::wait(process);
    delete receive;
    delete process;

    return 0;
  }

  int teardown()
  {
    if (initialize().isSome()) {
      return 1;
    }

    ShutdownMessage message;
    process::post(pid, message);
    sleep(1);

    return 0;
  }

  // Recover all containerized executors states.
  int recover()
  {
    Option<Error> result = thunk<RecoverRequest, RecoverResult>(false);
    if (result.isSome()) {
      cerr << "Recover failed: " << result.get().message << endl;
      return 1;
    }
    return 0;
  }

  // Start a containerized executor. Expects to receive an Launch
  // protobuf via stdin.
  int launch()
  {
    Option<Error> result = thunk<Launch, LaunchResult>();
    if (result.isSome()) {
      cerr << "Launch failed: " << result.get().message
           << endl;
      return 1;
    }
    return 0;
  }

  // Get the containerized executor's Termination.
  // Delivers a Termination protobuf filled with the information
  // gathered from launch's wait via stdout.
  int wait()
  {
    Option<Error> result = thunk<Wait, WaitResult>();
    if (result.isSome()) {
      cerr << "Wait failed: " << result.get().message
           << endl;
      return 1;
    }
    return 0;
  }

  // Update the container's resources.
  // Expects to receive a Update protobuf via stdin.
  int update()
  {
    Option<Error> result = thunk<Update, UpdateResult>();
    if (result.isSome()) {
      cerr << "Update failed: " << result.get().message
           << endl;
      return 1;
    }
    return 0;
  }

  // Gather resource usage statistics for the containerized executor.
  // Delivers an ResourceStatistics protobut via stdout when
  // successful.
  int usage()
  {
    Option<Error> result = thunk<Usage, UsageResult>();
    if (result.isSome()) {
      cerr << "Usage failed: " << result.get().message << endl;
      return 1;
    }
    return 0;
  }

  //
  int containers()
  {
    Option<Error> result = thunk<ContainersRequest, ContainersResult>(false);
    if (result.isSome()) {
      cerr << "Containers failed: " << result.get().message << endl;
      return 1;
    }
    return 0;
  }

  // Terminate the containerized executor.
  int destroy()
  {
    Option<Error> result = thunk<Destroy, DestroyResult>();
    if (result.isSome()) {
      cerr << "Destroy failed: " << result.get().message << endl;
      return 1;
    }
    return 0;
  }

private:
  PID<ReceiveProcess> pid;
  string path;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

void usage(const char* argv0, const hashset<string>& commands)
{
  cerr << "Usage: " << os::basename(argv0).get() << " <command>"
       << endl
       << endl
       << "Available commands:" << endl;

  foreach (const string& command, commands) {
    cerr << "    " << command << endl;
  }
}


int main(int argc, char** argv)
{
  using namespace mesos;
  using namespace internal;
  using namespace slave;

  hashmap<string, function<int()> > methods;

  TestContainerizer containerizer;

  // Daemon specific implementations.
  methods["setup"] = lambda::bind(
      &TestContainerizer::setup, &containerizer);
  methods["teardown"] = lambda::bind(
      &TestContainerizer::teardown, &containerizer);

  // Containerizer specific implementations.
  methods["recover"] = lambda::bind(
      &TestContainerizer::recover, &containerizer);
  methods["launch"] = lambda::bind(
      &TestContainerizer::launch, &containerizer);
  methods["wait"] = lambda::bind(
      &TestContainerizer::wait, &containerizer);
  methods["update"] = lambda::bind(
      &TestContainerizer::update, &containerizer);
  methods["usage"] = lambda::bind(
      &TestContainerizer::usage, &containerizer);
  methods["destroy"] = lambda::bind(
      &TestContainerizer::destroy, &containerizer);
  methods["containers"] = lambda::bind(
      &TestContainerizer::containers, &containerizer);

  if (argc != 2) {
    usage(argv[0], methods.keys());
    exit(1);
  }

  string command = argv[1];

  if (command == "--help" || command == "-h") {
    usage(argv[0], methods.keys());
    exit(0);
  }

  if (!methods.contains(argv[1])) {
    cerr << "'" << command << "' is not a valid command" << endl;
    usage(argv[0], methods.keys());
    exit(1);
  }

  return methods[command]();
}

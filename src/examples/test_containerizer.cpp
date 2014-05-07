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


// Communication process allowing the MesosContainerizerProcess to be
// controlled via proto messages.
// TODO(tillt): Consider renaming to "ThunkProcess" as the
// communication truely is two-way.
class ReceiveProcess : public ProtobufProcess<ReceiveProcess>
{
public:
  ReceiveProcess(MesosContainerizerProcess* target) : target(target) {}

  void initialize()
  {
    install<ShutdownMessage>(
        &ReceiveProcess::shutdown);

    install<LaunchRequest>(
        &ReceiveProcess::launch,
        &LaunchRequest::message);

    install<Update>(
        &ReceiveProcess::update,
        &Update::container_id,
        &Update::resources);

    install<Destroy>(
        &ReceiveProcess::destroy,
        &Destroy::container_id);

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

  // TODO(tillt): Find a way to combine all overloads into this or
  // refactor the common code into a seperate function.
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
      const Launch& message)
  {
    Future<Nothing> (MesosContainerizerProcess::*launch)(
      const ContainerID&,
      const TaskInfo&,
      const ExecutorInfo&,
      const string&,
      const Option<string>&,
      const SlaveID&,
      const PID<Slave>&,
      bool) = &MesosContainerizerProcess::launch;

    Option<string> userOption;
    if (!message.user().empty()) {
      userOption = message.user();
    }

    // TODO(tillt): This smells fishy - validate its function!
    PID<Slave> slave;
    std::stringstream stream;
    stream << message.slave_pid();
    stream >> slave;

    dispatch(
        target,
        launch,
        message.container_id(),
        message.task_info(),
        message.executor_info(),
        message.directory(),
        userOption,
        message.slave_id(),
        slave,
        message.checkpoint())
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
        &MesosContainerizerProcess::containers)
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
        &MesosContainerizerProcess::wait,
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
        &MesosContainerizerProcess::usage,
        containerId)
      .onAny(lambda::bind(
          &ReceiveProcess::reply<ResourceStatistics, UsageResult>,
          this,
          from,
          lambda::_1));
  }

  void destroy(const UPID& from, const ContainerID& containerId)
  {
    dispatch(
        target,
        &MesosContainerizerProcess::destroy,
        containerId);
  }

  void update(
      const UPID& from,
      const ContainerID& containerId,
      const vector<Resource>& resourceVector)
  {
    Resources resources;
    foreach(const Resource& resource, resourceVector) {
      resources += resource;
    }
    dispatch(
        target,
        &MesosContainerizerProcess::update,
        containerId,
        resources)
      .onAny(lambda::bind(
          &ReceiveProcess::reply<UpdateResult>,
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
  MesosContainerizerProcess* target;
};


// Allows spawning a daemon via "setup" and shutting it down again
// via "teardown".
// Also poses as the external interface of the ExternalContainerizer
// program via its standard API as defined within
// ExternalContainerizer.hpp.
// TOOD(tillt): Consider refactoring this into two classes.
class TestContainerizer
{
public:
  TestContainerizer() : workDir("/tmp/mesos-test-containerizer-daemon") {}
  virtual ~TestContainerizer() {}

  Option<Error> initialize()
  {
    try {
      std::ifstream file(path::join(workDir, "pid"));
      file >> pid;
      file.close();
    } catch(std::exception e) {
      return Error(string("Failed reading PID: ") + e.what());
    }
    return None();
  }

  // Transmit that message via socket to the MesosContainerizer,
  // receive the result message and pipe it out.
  template <typename T, typename R>
  Option<Error> thunk(const T& t)
  {
    Option<Error> init = initialize();
    if (init.isSome()) {
      return Error("Failed to initialize: " + init.get().message);
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

    // Send the payload message via pipe.
    Try<Nothing> sent = send(r.get().result());
    if (sent.isError()) {
      return Error("Failed to send to pipe: " + sent.error());
    }

    return None();
  }


  // Receive message via pipe if available, transmit that message via
  // socket to the MesosContainerizer, receive the result message.
  // TODO(tillt): Combine with the above function.
  template <typename T, typename R>
  Option<Error> oneWayThunk(const T& t)
  {
    Option<Error> init = initialize();
    if (init.isSome()) {
      return Error("Failed to initialize: " + init.get().message);
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

    return None();
  }


  // Prepare the MesosContainerizerProcess to use posix-isolation,
  // setup the process IO-API and serialize the UPID to the filesystem.
  int setup()
  {
    Flags flags;

    os::mkdir(workDir);

    flags.work_dir = workDir;

    flags.launcher_dir = path::join(BUILD_DIR, "src");

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

    // We now spawn two processes in this context; the subclassed
    // MesosContainerizerProcess (TestContainerizerProcess) as well
    // as a ProtobufProcess (ReceiveProcess) for covering the
    // communication with the former.

    MesosContainerizerProcess* process = new MesosContainerizerProcess(
        flags, true, Owned<Launcher>(launcher.get()), isolators);

    spawn(process, false);

    ReceiveProcess* receive = new ReceiveProcess(process);
    spawn(receive, false);

    pid = PID<ReceiveProcess>(receive);

    // Serialize the UPID.
    try {
      std::fstream file(path::join(workDir, "pid"), std::ios::out);
      file << pid;
      file.close();
    } catch(std::exception e) {
      cerr << "Failed writing PID: " << e.what() << endl;
      return 1;
    }

    cerr << "PID: " << pid << endl;

    // Now keep running until we get terminated via teardown.
    process::wait(process);

    delete receive;
    delete process;

    return 0;
  }

  // Shutdown the daemon.
  int teardown()
  {
    Option<Error> init = initialize();
    if (init.isSome()) {
      cerr << "Failed to initialize: " << init.get().message << endl;
      return 1;
    }

    ShutdownMessage message;
    process::post(pid, message);
    cerr << "Sending shutdown message.." << endl;

    sleep(1);

    Try<Nothing> rmdir = os::rmdir(workDir);
    if (rmdir.isError()) {
      cerr << "Failed to remove '" << workDir << "': "
           << rmdir.error() << endl;
    }

    return 0;
  }

  // Recover all containerized executors states.
  int recover()
  {
    // This implementation does not persist any states, hence it does
    // need or support an internal recovery.
    return 0;
  }

  // Start a containerized executor. Expects to receive an Launch
  // protobuf via stdin.
  int launch()
  {
    Result<Launch> received = receive<Launch>();
    if (received.isError()) {
      cerr << "Failed to receive from pipe: " << received.error() << endl;
      return 1;
    }

    // We need to wrap the Launch message as "install" only supports
    // up to 6 parameters whereas the Launch message has 8 members.
    LaunchRequest wrapped;
    wrapped.mutable_message()->CopyFrom(received.get());

    Option<Error> result = oneWayThunk<LaunchRequest, LaunchResult>(wrapped);
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
    Result<Wait> received = receive<Wait>();
    if (received.isError()) {
      cerr << "Failed to receive from pipe: " << received.error() << endl;
      return 1;
    }

    Option<Error> result = thunk<Wait, WaitResult>(received.get());
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
    Result<Update> received = receive<Update>();
    if (received.isError()) {
      cerr << "Failed to receive from pipe: " << received.error() << endl;
      return 1;
    }
    Option<Error> result = oneWayThunk<Update, UpdateResult>(received.get());
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
    Result<Usage> received = receive<Usage>();
    if (received.isError()) {
      cerr << "Failed to receive from pipe: " << received.error() << endl;
      return 1;
    }
    Option<Error> result = thunk<Usage, UsageResult>(received.get());
    if (result.isSome()) {
      cerr << "Usage failed: " << result.get().message << endl;
      return 1;
    }
    return 0;
  }

  // Receive active containers.
  int containers()
  {
    ContainersRequest request;
    Option<Error> result = thunk<
      ContainersRequest, ContainersResult>(request);
    if (result.isSome()) {
      cerr << "Containers failed: " << result.get().message << endl;
      return 1;
    }
    return 0;
  }

  // Terminate the containerized executor.
  int destroy()
  {
    Option<Error> init = initialize();
    if (init.isSome()) {
      cerr << "Failed to initialize: " + init.get().message << endl;
      return 1;
    }

    // Receive the message via pipe, if needed.
    Result<Destroy> received = receive<Destroy>();
    if (received.isError()) {
      cerr << "Failed to receive from pipe: " + received.error() << endl;
      return 1;
    }

    process::post(pid, received.get());

    return 0;
  }

private:
  PID<ReceiveProcess> pid;
  string workDir;
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

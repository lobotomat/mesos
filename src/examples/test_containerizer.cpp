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

#include <sys/resource.h>


#include <stdlib.h>     // exit

#include <iostream>
#include <fstream>
#include <string>

#include <mesos/mesos.hpp>
#include <mesos/mesos.hpp>
#include <mesos/containerizer/containerizer.hpp>

#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include <stout/hashset.hpp>
#include <stout/flags.hpp>
#include <stout/lambda.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>

#include "examples/test_containerizer.pb.h"

#include "messages/messages.hpp"

#include "slave/flags.hpp"

#include "slave/containerizer/mesos_containerizer.hpp"

#include "slave/containerizer/isolators/posix.hpp"


using namespace mesos;
using namespace mesos::internal::slave;
using namespace mesos::containerizer;
using namespace mesos::containerizer::examples;

using namespace process;

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::vector;


namespace protocol {

Protocol<LaunchRequest, LaunchResult> launch;
Protocol<Wait, WaitResult> wait;
Protocol<Update, UpdateResult> update;
Protocol<Usage, UsageResult> usage;
Protocol<ContainersRequest, ContainersResult> containers;

} // namespace protocol {


string thunkDirectory(const string& directory)
{
  return path::join(directory, "test_containerizer");
}


string fifoPath(const string& directory)
{
  string baseDir = thunkDirectory(directory);
  return path::join(baseDir, "fifo");
}


string pidPath(const string& directory)
{
  string baseDir = thunkDirectory(directory);
  return path::join(baseDir, "pid");
}


string daemonDirectory(const string& directory)
{
  string baseDir = thunkDirectory(directory);
  return path::join(baseDir, "daemon");
}


string daemonStderrPath(const string& directory)
{
  string baseDir = daemonDirectory(directory);
  return path::join(baseDir, "stderr");
}


// Receive a record-io protobuf message via stdin.
template<typename T>
Result<T> receive()
{
  return ::protobuf::read<T>(STDIN_FILENO, false, false);
}


// Send a record-io protobuf message via stdout.
Try<Nothing> send(const google::protobuf::Message& message)
{
  return ::protobuf::write(STDOUT_FILENO, message);
}


// Verify the future status of a protobuf reception and check the
// container (payload) FutureResult.status. When both are ready,
// return the container.
template<typename T>
Try<T> validate(const Future<T>& future)
{
  if (!future.isReady()) {
    return Error("Outer result future " +
        (future.isFailed() ? "failed: " + future.failure()
                           : "got discarded"));
  }

  T result = future.get();
  if (result.future().status() != examples::FUTURE_READY) {
    return Error("Inner result future (protobuffed) " +
        (result.future().status() ==  examples::FUTURE_FAILED
            ? "failed: " + future.failure()
            : "got discarded"));
  }

  return result;
}


Option<Error> daemonize(const string& directory, const string& argv0)
{
  VLOG(1) << "Forking daemon....";

  // Create test-comtainerizer work directory.
  CHECK_SOME(os::mkdir(thunkDirectory(directory)))
    << "Failed to create test-containerizer work directory '"
    << thunkDirectory(directory) << "'";

  CHECK_SOME(os::mkdir(daemonDirectory(directory)))
    << "Failed to create daemon directory '"
    << daemonDirectory(directory) << "'";

  // Create a named pipe for syncing parent and child process.
  if (mkfifo(fifoPath(directory).c_str(), 0666) < 0) {
    return ErrnoError("Failed to create fifo");
  }

  string command = argv0 + " setup 2>" + daemonStderrPath(directory);
  VLOG(2) << "exec: " << command;

  // Spawn the process and wait for it in a child process.
  int pid = ::fork();
  if (pid == -1) {
    return ErrnoError("Failed to fork");;
  }
  if (pid == 0) {
    execl("/bin/sh", "sh", "-c", command.c_str(), (char*) NULL);
    ABORT("exec failed");
  }

  // We are in the parent context.
  // Sync parent and child process.
  int pipe = open(fifoPath(directory).c_str(), O_RDONLY);
  if (pipe < 0) {
    return ErrnoError("Failed open fifo");
  }
  int sync;
  while (::read(pipe, &sync, sizeof(sync)) == -1 &&
         errno == EINTR);
  close(pipe);

  return None();
}


// Communication process allowing the MesosContainerizerProcess to be
// controlled via remote proto messages.
class ThunkProcess : public ProtobufProcess<ThunkProcess>
{
public:
  explicit ThunkProcess(
      MesosContainerizerProcess* containerizer,
      const string& fifoPath)
  : containerizer(containerizer),
    garbageCollecting(false),
    fifoPath(fifoPath) {}

  virtual ~ThunkProcess() {}

private:
  void initialize()
  {
    link(containerizer->self());

    VLOG(2) << "Installing handlers";
    install<LaunchRequest>(
        &ThunkProcess::launch,
        &LaunchRequest::message);

    install<Update>(
        &ThunkProcess::update,
        &Update::container_id,
        &Update::resources);

    install<Destroy>(
        &ThunkProcess::destroy,
        &Destroy::container_id);

    install<ContainersRequest>(
        &ThunkProcess::containers);

    install<Wait>(
        &ThunkProcess::wait,
        &Wait::container_id);

    install<Usage>(
        &ThunkProcess::usage,
        &Usage::container_id);

    VLOG(2) << "Fully up and running";

    int pipe = open(fifoPath.c_str(), O_WRONLY);
    if (pipe < 0) {
      LOG(ERROR) << "Failed open fifo";
      return;
    }
    // Sync parent and child process.
    int sync = 42;
    while (::write(pipe, &sync, sizeof(sync)) == -1 &&
           errno == EINTR);
    close(pipe);

    VLOG(2) << "Synced with client invocation";
  }

  void finalize()
  {
    VLOG(1) << "Shutdown containerizer";

    // Join the MesosContainerizer process.
    terminate(containerizer->self());
  }

  void shutdown()
  {
    VLOG(1) << "Shutdown initiated";

    // Suicide.
    terminate(this->self());

    VLOG(1) << "Shutdown done, byebye!";
  }

  void startGarbageCollecting()
  {
    if (garbageCollecting) {
      return;
    }
    VLOG(1) << "Started garbage collection";
    garbageCollecting = true;
    garbageCollect();
  }

  void garbageCollect()
  {
    VLOG(1) << "Checking containers for garbage collection";
    dispatch(
        containerizer->self(),
        &MesosContainerizerProcess::containers)
      .onReady(lambda::bind(
          &Self::_garbageCollect,
          this,
          lambda::_1));
  }

  void _garbageCollect(const hashset<ContainerID>& containers)
  {
    if(containers.size()) {
      VLOG(2) << "We still have containers active" << endl;
      // Garbage collect forever.
      process::delay(Seconds(1), self(), &Self::garbageCollect);
      return;
    }

    VLOG(1) << "No more containers active, terminate!";

    // When no containers are active, shutdown.
    shutdown();
  }

  // Future<hashset<ContainerID> > overload.
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
      Containers* containers = r.mutable_result();
      foreach(const ContainerID& containerId, future.get()) {
        containers->add_containers()->CopyFrom(containerId);
      }
    }

    VLOG(2) << "Sending reply: " << r.DebugString();

    send(from, r);
  }

  // Future<Nothing> overload.
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

    VLOG(2) << "Sending reply: " << r.DebugString();

    send(from, r);
  }

  // Answer the original request with a protobuffed result created
  // from the dispatched future and its container.
  // Future<protobuf::Message> overload
  // TODO(tillt): Find a way to combine all overloads into this or
  // refactor the common code into a separate function.
  template<typename T, typename R>
  void reply(const UPID& from, const Future<T>& future)
  {
    FutureMessage message;

    // Convert the dispatch future status into a protobuf.
    message.set_status(future.isReady()
        ? FUTURE_READY
        : future.isFailed() ? FUTURE_FAILED
                            : FUTURE_DISCARDED);
    if (future.isFailed()) {
      message.set_message(future.failure());
    }

    // Wrap both, the Future status as well as its container into a
    // result protobuf.
    R r;
    r.mutable_future()->CopyFrom(message);
    if (future.isReady()) {
      r.mutable_result()->CopyFrom(future.get());
    }

    VLOG(2) << "Sending reply: " << r.DebugString();

    // Transmit the result back to the request process.
    send(from, r);
  }

  void launch(
      const UPID& from,
      const Launch& message)
  {
    VLOG(1) << "Received launch message";
    Future<Nothing> (
        MesosContainerizerProcess::*launch)(
            const ContainerID&,
            const TaskInfo&,
            const ExecutorInfo&,
            const string&,
            const Option<string>&,
            const SlaveID&,
            const PID<Slave>&,
            bool) = &MesosContainerizerProcess::launch;

    // TODO(tillt): This smells fishy - validate its function!
    PID<Slave> slave;
    std::stringstream stream;
    stream << message.slave_pid();
    stream >> slave;

    dispatch(
        containerizer->self(),
        launch,
        message.container_id(),
        message.task_info(),
        message.executor_info(),
        message.directory(),
        message.user().empty() ? None() : Option<string>(message.user()),
        message.slave_id(),
        slave,
        false)
      .onAny(lambda::bind(
        &Self::reply<LaunchResult>,
        this,
        from,
        lambda::_1))
      .onReady(lambda::bind(
        &Self::startGarbageCollecting,
        this));
  }

  void containers(const UPID& from)
  {
    VLOG(1) << "Received containers message";
    void (ThunkProcess::*reply)(
        const UPID&,
        const Future<hashset<ContainerID> >&) = &ThunkProcess::reply;

    dispatch(
        containerizer->self(),
        &MesosContainerizerProcess::containers)
      .onAny(lambda::bind(
          reply,
          this,
          from,
          lambda::_1));
  }

  void wait(const UPID& from, const ContainerID& containerId)
  {
    VLOG(1) << "Received wait message";
    dispatch(
        containerizer->self(),
        &MesosContainerizerProcess::wait,
        containerId)
      .onAny(lambda::bind(
          &ThunkProcess::reply<Termination, WaitResult>,
          this,
          from,
          lambda::_1));
  }

  void usage(const UPID& from, const ContainerID& containerId)
  {
    VLOG(1) << "Received usage message";
    dispatch(
        containerizer->self(),
        &MesosContainerizerProcess::usage,
        containerId)
      .onAny(lambda::bind(
          &ThunkProcess::reply<ResourceStatistics, UsageResult>,
          this,
          from,
          lambda::_1));
  }

  void destroy(const UPID& from, const ContainerID& containerId)
  {
    VLOG(1) << "Received destroy message";
    dispatch(
        containerizer->self(),
        &MesosContainerizerProcess::destroy,
        containerId);
  }

  void update(
      const UPID& from,
      const ContainerID& containerId,
      const vector<Resource>& resourceVector)
  {
    VLOG(1) << "Received update message";
    Resources resources;
    foreach(const Resource& resource, resourceVector) {
      resources += resource;
    }
    dispatch(
        containerizer->self(),
        &MesosContainerizerProcess::update,
        containerId,
        resources)
      .onAny(lambda::bind(
          &ThunkProcess::reply<UpdateResult>,
          this,
          from,
          lambda::_1));
  }

  MesosContainerizerProcess* containerizer;
  bool garbageCollecting;
  const string& fifoPath;
};


Try<MesosContainerizerProcess*> createContainerizer(
    const string& workDir)
{
  Flags flags;
  flags.work_dir = workDir;
  flags.launcher_dir = path::join(BUILD_DIR, "src");
  //flags.launcher_dir = path::join(tests::flags.build_dir, "src");

  // Create a MesosContainerizerProcess using isolators and a launcher.
  vector<Owned<Isolator> > isolators;

  Try<Isolator*> cpuIsolator = PosixCpuIsolatorProcess::create(flags);
  if (cpuIsolator.isError()) {
    return Error("Could not create PosixCpuIsolator: " +
        cpuIsolator.error());
  }
  isolators.push_back(Owned<Isolator>(cpuIsolator.get()));

  Try<Isolator*> memIsolator = PosixMemIsolatorProcess::create(flags);
  if (memIsolator.isError()) {
    return Error("Could not create PosixMemIsolator: " +
        memIsolator.error());
  }
  isolators.push_back(Owned<Isolator>(memIsolator.get()));

  Try<Launcher*> launcher = PosixLauncher::create(flags);
  if (launcher.isError()) {
    return Error("Could not create PosixLauncher: " +
        launcher.error());
  }

  // Contstruct the MesosContainerizerProcess.
  // We need to use the local=false argument to enable the mesos
  // containerizer redirecting stdout and stderr towards the log-
  // files. If that is not done, the pipe communication of the
  // ExternalContainerizer is getting garbled.
  MesosContainerizerProcess* containerizer =
    new MesosContainerizerProcess(
        flags,
        false,
        Owned<Launcher>(launcher.get()),
        isolators);

  return containerizer;
}


// Get the PID of a running daemon.
Try<PID<ThunkProcess> > thunkPid(const string& directory)
{
  // An existing pid-file signals that the daemon is active.
  if (!os::isfile(pidPath(directory))) {
    return Error("PID-file does not exist");
  }

  PID<ThunkProcess> pid;
  try {
    std::ifstream file(pidPath(directory));
    file >> pid;
    file.close();
  } catch(std::exception e) {
    return Error(string("Failed reading PID: ") + e.what());
  }
  VLOG(2) << "Existing daemon pid: " << pid;
  return pid;
}


int setup(const string& directory)
{
  Try<MesosContainerizerProcess*> containerizer =
    createContainerizer(directory);
  if (containerizer.isError()) {
    LOG(ERROR) << "Failed to create MesosContainerizerProcess: "
               << containerizer.error();
    return 1;
  }

  // Spawn the containerizer process.
  process::spawn(containerizer.get(), true);
  LOG(INFO) << "Containerizer " << containerizer.get()->self() << " running";

  // Create our ThunkProcess, wrapping the containerizer process.
  ThunkProcess* process = new ThunkProcess(
      containerizer.get(), fifoPath(directory));

  // Serialize the PID to the filesystem.
  try {
    std::fstream file(pidPath(directory), std::ios::out);
    file << process->self();
    file.close();
  } catch(std::exception e) {
    LOG(ERROR) << "Failed writing PID: " << e.what();
    return 1;
  }

  // Run until we get terminated via teardown.
  process::spawn(process, true);
  LOG(INFO) << "Daemon " << process->self() << " running";

  process::wait(process);
  LOG(INFO) << "Daemon terminated";

  Try<Nothing> rmFifo = os::rm(fifoPath(directory));
  if (rmFifo.isError()) {
    LOG(ERROR) << "Failed to remove '" << fifoPath(directory) << "': "
               << rmFifo.error();
    return 1;
  }
  Try<Nothing> rmPid = os::rm(pidPath(directory));
  if (rmPid.isError()) {
    LOG(ERROR) << "Failed to remove '" << pidPath(directory) << "': "
               << rmPid.error();
    return 1;
  }
  os::rmdir(thunkDirectory(directory));

  return 0;
}


// Recover all containerized executors states.
int recover(const Option<PID<ThunkProcess> >& pid)
{
  // This implementation does not persist any states, hence it does
  // need or support an internal recovery.
  return 0;
}


// Start a containerized executor. Expects to receive an Launch
// protobuf via stdin.
int launch(const Option<PID<ThunkProcess> >& pid)
{
  if (pid.isNone()) {
    LOG(ERROR) << "Launch needs a PID to talk to";
    return 1;
  }

  Result<Launch> received = receive<Launch>();
  if (received.isError()) {
    LOG(ERROR) << "Failed to receive from pipe: " << received.error();
    return 1;
  }

  // We need to wrap the Launch message as "install" only supports
  // up to 6 parameters whereas the Launch message has 8 members.
  LaunchRequest wrapped;
  wrapped.mutable_message()->CopyFrom(received.get());

  Future<LaunchResult> future = protocol::launch(pid.get(), wrapped);

  future.await();

  Try<LaunchResult> result = validate(future);
  if (result.isError()) {
    LOG(ERROR) << "Exchange failed: " + result.error();
    return 1;
  }

  return 0;
}


// Get the containerized executor's Termination.
// Delivers a Termination protobuf filled with the information
// gathered from launch's wait via stdout.
int wait(const Option<PID<ThunkProcess> >& pid)
{
  if (pid.isNone()) {
    LOG(ERROR) << "Wait needs a PID to talk to";
    return 1;
  }

  Result<Wait> received = receive<Wait>();
  if (received.isError()) {
    LOG(ERROR) << "Failed to receive from pipe: " << received.error();
    return 1;
  }

  Future<WaitResult> future = protocol::wait(pid.get(), received.get());

  future.await();

  Try<WaitResult> result = validate(future);
  if (result.isError()) {
    LOG(ERROR) << "Exchange failed: " + result.error();
    return 1;
  }

  Try<Nothing> sent = send(result.get().result());
  if (sent.isError()) {
    LOG(ERROR) << "Failed to send to pipe: " + sent.error();
    return 1;
  }

  return 0;
}


// Update the container's resources.
// Expects to receive a Update protobuf via stdin.
int update(const Option<PID<ThunkProcess> >& pid)
{
  if (pid.isNone()) {
    LOG(ERROR) << "Update needs a PID to talk to";
    return 1;
  }

  Result<Update> received = receive<Update>();
  if (received.isError()) {
    LOG(ERROR) << "Failed to receive from pipe: " << received.error();
    return 1;
  }

  Future<UpdateResult> future = protocol::update(pid.get(), received.get());

  future.await();

  Try<UpdateResult> result = validate(future);
  if (result.isError()) {
    LOG(ERROR) << "Exchange failed: " + result.error();
    return 1;
  }

  return 0;
}


// Gather resource usage statistics for the containerized executor.
// Delivers an ResourceStatistics protobut via stdout when
// successful.
int usage(const Option<PID<ThunkProcess> >& pid)
{
  if (pid.isNone()) {
    LOG(ERROR) << "Usage needs a PID to talk to";
    return 1;
  }

  Result<Usage> received = receive<Usage>();
  if (received.isError()) {
    LOG(ERROR) << "Failed to receive from pipe: " << received.error();
    return 1;
  }

  Future<UsageResult> future = protocol::usage(pid.get(), received.get());

  future.await();

  Try<UsageResult> result = validate(future);
  if (result.isError()) {
    LOG(ERROR) << "Exchange failed: " + result.error();
    return 1;
  }

  Try<Nothing> sent = send(result.get().result());
  if (sent.isError()) {
    LOG(ERROR) << "Failed to send to pipe: " + sent.error();
    return 1;
  }

  return 0;
}


// Receive active containers.
int containers(const Option<PID<ThunkProcess> >& pid)
{
  // We may be asked for containers even if we never received a
  // launch on this slave.
  if (pid.isNone()) {
    Containers containers;
    Try<Nothing> sent = send(containers);
    if (sent.isError()) {
      LOG(ERROR) << "Failed to send to pipe: " << sent.error();
      return 1;
    }
    return 0;
  }

  ContainersRequest request;
  Future<ContainersResult> future = protocol::containers(pid.get(), request);

  future.await();

  Try<ContainersResult> result = validate(future);
  if (result.isError()) {
    LOG(ERROR) << "Exchange failed: " + result.error();
    return 1;
  }

  Try<Nothing> sent = send(result.get().result());
  if (sent.isError()) {
    LOG(ERROR) << "Failed to send to pipe: " + sent.error();
    return 1;
  }

  return 0;
}


// Terminate the containerized executor.
int destroy(const Option<PID<ThunkProcess> >& pid)
{
  if (pid.isNone()) {
    LOG(ERROR) << "Destroy needs a PID to talk to";
    return 1;
  }

  // Receive the message via pipe.
  Result<Destroy> received = receive<Destroy>();
  if (received.isError()) {
    LOG(ERROR) << "Failed to receive from pipe: " + received.error();
    return 1;
  }

  // Destroy does not return a future, hence we can not validate its
  // results, thus we do a simple fire-and-forget post.
  process::post(pid.get(), received.get());

  return 0;
}


void usage(const char* argv0, const hashset<string>& commands)
{
  cout << "Usage: " << os::basename(argv0).get() << " <command>"
       << endl
       << endl
       << "Available commands:" << endl;

  foreach (const string& command, commands) {
    cout << "    " << command << endl;
  }
}


bool enableCoreDumps(void)
{
  struct rlimit limit;

  limit.rlim_cur = RLIM_INFINITY;
  limit.rlim_max = RLIM_INFINITY;

  return setrlimit(RLIMIT_CORE, &limit) == 0;
}


int main(int argc, char** argv)
{
  hashmap<string, int(*)(const Option<PID<ThunkProcess> >&)> methods;

  methods["setup"] = NULL;
  methods["recover"] = recover;
  methods["launch"] = launch;
  methods["wait"] = wait;
  methods["update"] = update;
  methods["usage"] = usage;
  methods["destroy"] = destroy;
  methods["containers"] = containers;

  if (argc != 2) {
    usage(argv[0], methods.keys());
    exit(1);
  }

  string command = argv[1];

  if (command == "--help" || command == "-h") {
    usage(argv[0], methods.keys());
    exit(0);
  }

  if (!methods.contains(command)) {
    cerr << "'" << command << "' is not a valid command" << endl;
    usage(argv[0], methods.keys());
    exit(1);
  }

  enableCoreDumps();

  CHECK(os::hasenv("MESOS_WORK_DIRECTORY"))
    << "Missing MESOS_WORK_DIRECTORY environment variable";
  string directory = os::getenv("MESOS_WORK_DIRECTORY");

  VLOG(2) << "MESOS_WORK_DIRECTORY: " << directory;

  VLOG(2) << "Invoking " << command << " command";

  int ret;
  if (command == "setup") {
    ret = setup(directory);
  } else {
    // Launch implicitely forks a daemon if needed.
    if (command == "launch") {
      if (!os::isfile(pidPath(directory))) {
        Option<Error> daemonized = daemonize(directory, argv[0]);
        if (daemonized.isSome()) {
          cerr << daemonized.get().message << endl;
        }
      }
    }

    // Deserialize the PID of our daemon.
    Try<PID<ThunkProcess> > tried = thunkPid(directory);
    if (tried.isError()) {
      LOG(WARNING) << "Could not get PID for daemon";
    }

    Option<PID<ThunkProcess> > pid =
      tried.isError() ? None()
                      : Option<PID<ThunkProcess> >(tried.get());

    // Run the command.
    ret = methods[command](pid);
  }

  VLOG(1) << command  << " has completed with status: " << ret << endl;

  return ret;
}

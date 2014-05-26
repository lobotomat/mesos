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

// The test-containerizer is posing as a external containerizer by
// using the MesosContainerizerProcess (MC) to handle the actual
// containerizing.
// The MC is wrapped by a protobuf process based communication
// interface (thunk). Both processes are run in form of a daemon.
// The daemon is controlled by invocations of this test-containerizer
// as a trigger executable.
//
// Both processes are spawned implicitly whenever "launch" is invoked
// on a MESOS_WORK_DIRECTORY that does not have a daemon running yet.
// The daemon terminates itself whenever there are no more active
// containers according to the MesosContainerizerProcess.
//
// The resulting communication scheme is as follows:
// ExternalContainerizer-trigger-thunk-MesosContainerizerProcess

#include <stdlib.h>

#include <sys/file.h>

#include <iostream>
#include <fstream>
#include <string>

#include <mesos/mesos.hpp>
#include <mesos/containerizer/containerizer.hpp>

#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include <stout/flags.hpp>
#include <stout/hashset.hpp>
#include <stout/lambda.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>

#include "messages/messages.hpp"

#include "slave/flags.hpp"

#include "slave/containerizer/mesos_containerizer.hpp"

#include "slave/containerizer/isolators/posix.hpp"

#include "tests/test_containerizer.pb.h"


using namespace mesos;
using namespace mesos::containerizer;
using namespace mesos::containerizer::tests;
using namespace mesos::internal::slave;

using namespace process;

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::vector;

using lambda::function;


// Slave configuration specific (MESOS_WORK_DIRECTORY) path getters.
string thunkDirectory(const string& directory)
{
  return path::join(directory, "test_containerizer");
}


string fifoPath(const string& directory)
{
  return path::join(thunkDirectory(directory), "fifo");
}


string pidPath(const string& directory)
{
  return path::join(thunkDirectory(directory), "pid");
}


string stderrPath(const string& directory)
{
  return path::join(thunkDirectory(directory), "stderr");
}


string lockPath(const string& directory)
{
  return path::join(thunkDirectory(directory), "pid_lock");
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


// Allows you to describe request/response protocols and then use
// those for sending requests and getting back responses. This variant
// of Protocol is working synchronously by blocking until the
// communication process has terminated.
template <typename Req, typename Res>
struct SyncProtocol
{
  Try<Res> operator () (
      const UPID& pid,
      const Req& req) const
  {
    // Help debugging by adding some "type constraints".
    { Req* req = NULL; google::protobuf::Message* m = req; (void)m; }
    { Res* res = NULL; google::protobuf::Message* m = res; (void)m; }

    ReqResProcess<Req, Res>* process = new ReqResProcess<Req, Res>(pid, req);
    spawn(process, false);

    Future<Res> future = dispatch(process, &ReqResProcess<Req, Res>::run);

    wait(process);

    delete process;

    if (!future.isReady()) {
      return Error("Failed to receive a result");
    }

    return future.get();
  }
};


// Communication process allowing the MesosContainerizerProcess to be
// controlled via remote proto messages.
class ThunkProcess : public ProtobufProcess<ThunkProcess>
{
public:
  explicit ThunkProcess(
      MesosContainerizerProcess* containerizer,
      const string& directory)
  : containerizer(containerizer),
    directory(directory),
    garbageCollecting(false) {}

  virtual ~ThunkProcess() {}

protected:
  virtual void initialize()
  {
    try {
      std::fstream file(pidPath(directory), std::ios::out);
      file << self();
      file.close();
    } catch(std::exception e) {
      cerr << "Failed writing PID: " << e.what() << endl;
    }

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

    // Sync parent and child process.
    Try<int> pipe = os::open(fifoPath(directory).c_str(), O_WRONLY);
    if (pipe.isError()) {
      cerr << "Failed open fifo" << pipe.error() << endl;
      return;
    }
    int sync = 0;
    while (::write(pipe.get(), &sync, sizeof(sync)) == -1 &&
           errno == EINTR);
    os::close(pipe.get());
  }

  virtual void finalize()
  {
    Try<Nothing> rmPid = os::rm(pidPath(directory));
    if (rmPid.isError()) {
      cerr << "Failed to remove '" << pidPath(directory)
           << "': " << rmPid.error() << endl;
    }
  }

private:
  // Initiates a periodic check for active containers on the
  // MesosContainerizerProcess.
  void startGarbageCollecting()
  {
    if (garbageCollecting) {
      return;
    }
    garbageCollecting = true;
    garbageCollect();
  }

  // Asks the MesosContainerizerProcess for active containers.
  void garbageCollect()
  {
    dispatch(
        containerizer->self(),
        &MesosContainerizerProcess::containers)
      .onReady(lambda::bind(
          &Self::_garbageCollect,
          this,
          lambda::_1));
  }

  // Checks the containers result and if none are left, initiates a
  // termination of both processes.
  void _garbageCollect(const hashset<ContainerID>& containers)
  {
    // When no containers are active, shutdown.
    if(containers.empty()) {
      terminate(containerizer->self());
      terminate(this->self());
      return;
    }
    // Garbage collect forever.
    delay(Seconds(1), self(), &Self::garbageCollect);
  }

  // Convert the dispatch future status into a protobuf.
  template<typename T>
  FutureMessage futureMessage(const Future<T>& future)
  {
    FutureMessage message;
    message.set_status(
        future.isReady() ? FUTURE_READY
                         : future.isFailed() ? FUTURE_FAILED
                                             : FUTURE_DISCARDED);
    if (future.isFailed()) {
      message.set_message(future.failure());
    }
    return message;
  }

  // Answer the original request with a protobuffed result created
  // from the dispatched future and its container (payload).
  // Future<hashset<ContainerID> > overload.
  void reply(const UPID& from, const Future<hashset<ContainerID> >& future)
  {
    ContainersResult result;
    result.mutable_future()->CopyFrom(futureMessage(future));
    if (future.isReady()) {
      Containers* containers = result.mutable_result();
      foreach(const ContainerID& containerId, future.get()) {
        containers->add_containers()->CopyFrom(containerId);
      }
    }
    send(from, result);
  }

  // Future<Nothing> overload.
  template<typename Res>
  void reply(const UPID& from, const Future<Nothing>& future)
  {
    Res result;
    result.mutable_future()->CopyFrom(futureMessage(future));
    send(from, result);
  }

  // Future<protobuf::Message> overload
  template<typename T, typename Res>
  void reply(const UPID& from, const Future<T>& future)
  {
    Res result;
    result.mutable_future()->CopyFrom(futureMessage(future));
    if (future.isReady()) {
      result.mutable_result()->CopyFrom(future.get());
    }
    send(from, result);
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
    Resources resources;
    foreach (const Resource& resource, resourceVector) {
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
  // Slave configuration specific work directory.
  const string& directory;
  // Garbage collection activity indicator.
  bool garbageCollecting;
};


// Prepare the MesosContainerizerProcess to use posix-isolation,
Try<MesosContainerizerProcess*> createContainerizer(
    const string& workDir)
{
  Flags flags;
  flags.work_dir = workDir;
  flags.launcher_dir = path::join(BUILD_DIR, "src");

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

  // Construct the MesosContainerizerProcess.
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


// Tries to get the PID of a running daemon.
Try<PID<ThunkProcess> > thunkPid(const string& directory)
{
  // An existing pid-file signals that the daemon is active.
  if (!os::isfile(pidPath(directory))) {
    return Error("PID-file does not exist");
  }

  PID<ThunkProcess> pid;
  try {
    std::fstream file(pidPath(directory), std::ios::in);
    file >> pid;
    file.close();
  } catch(std::exception e) {
    return Error(string("Failed reading PID: ") + e.what());
  }

  return pid;
}


// Transmit a message via socket to the MesosContainerizerProcess,
// block until a result message is received.
template <typename T, typename R>
Try<R> thunk(const string& directory, const T& t)
{
  Try<PID<ThunkProcess> > pid = thunkPid(directory);
  if (pid.isError()) {
    return Error("Failed to initialize: " + pid.error());
  }

  // Send a request and receive an answer via process protobuf
  // exchange.
  SyncProtocol<T, R> protocol;
  return protocol(pid.get(), t);
}


// Transmit a message via socket to the MesosContainerizerProcess,
// block until a result message is received and pipe it out, back
// to the ExternalContainerizer.
template <typename T, typename R>
Option<Error> thunkOut(const string& directory, const T& t)
{
  Try<R> result = thunk<T, R>(directory, t);
  if (result.isError()) {
    return result.error();
  }

  // Send the payload message via pipe.
  Try<Nothing> sent = send(result.get().result());
  if (sent.isError()) {
    return Error("Failed to send to pipe: " + sent.error());
  }

  return None();
}


// Initialize and spawn both, the MesosContainerizerProcess as well
// as the wrapping communication interface, the ThunkProcess.
// This call blocks until the ThunkProcess has been terminated.
int setup(const string& directory)
{
  Try<MesosContainerizerProcess*> containerizer =
    createContainerizer(directory);
  if (containerizer.isError()) {
    cerr << "Failed to create MesosContainerizerProcess: "
         << containerizer.error() << endl;
    return 1;
  }

  // Spawn the containerizer process.
  MesosContainerizerProcess* container = containerizer.get();
  spawn(container, true);

  // Create our ThunkProcess, wrapping the containerizer process.
  ThunkProcess* process = new ThunkProcess(container, directory);

  // Run until we get terminated via garbage collection.
  spawn(process, true);
  wait(process);

  // Cleanup artefacts.
  Try<Nothing> rmFifo = os::rm(fifoPath(directory));
  if (rmFifo.isError()) {
    cerr << "Failed to remove '" << fifoPath(directory)
         << "': " << rmFifo.error() << endl;
  }

  Try<Nothing> rmLock = os::rm(lockPath(directory));
  if (rmLock.isError()) {
    cerr << "Failed to remove '" << lockPath(directory)
         << "': " << rmLock.error() << endl;
  }

  Try<Nothing> rmStderr = os::rm(stderrPath(directory));
  if (rmStderr.isError()) {
    cerr << "Failed to remove '" << stderrPath(directory)
         << "': " << rmStderr.error() << endl;
  }

  os::rmdir(thunkDirectory(directory));

  return 0;
}


// Recover all containerized executors states.
int recover(const string& directory)
{
  // This implementation does not persist any states, hence it does
  // need or support an internal recovery.
  return 0;
}


// Start a containerized executor. Expects to receive a Launch
// protobuf via stdin.
int launch(const string& directory)
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

  Try<LaunchResult> result =
    thunk<LaunchRequest, LaunchResult>(directory, wrapped);
  if (result.isError()) {
    cerr << "Launch thunking failed: " << result.error() << endl;
    return 1;
  }
  return 0;
}


// Get the containerized executor's Termination.
// Delivers a Termination protobuf filled with the information
// gathered from launch's wait via stdout.
int wait(const string& directory)
{
  Result<Wait> received = receive<Wait>();
  if (received.isError()) {
    cerr << "Failed to receive from pipe: " << received.error() << endl;
    return 1;
  }

  Option<Error> result =
    thunkOut<Wait, WaitResult>(directory, received.get());
  if (result.isSome()) {
    cerr << "Wait thunking failed: " << result.get().message << endl;
    return 1;
  }
  return 0;
}


// Update the container's resources.
// Expects to receive a Update protobuf via stdin.
int update(const string& directory)
{
  Result<Update> received = receive<Update>();
  if (received.isError()) {
    cerr << "Failed to receive from pipe: " << received.error() << endl;
    return 1;
  }

  Try<UpdateResult> result =
    thunk<Update, UpdateResult>(directory, received.get());
  if (result.isError()) {
    cerr << "Update thunking failed: " << result.error() << endl;
    return 1;
  }
  return 0;
}


// Gather resource usage statistics for the containerized executor.
// Delivers an ResourceStatistics protobuf via stdout when
// successful.
int usage(const string& directory)
{
  Result<Usage> received = receive<Usage>();
  if (received.isError()) {
    cerr << "Failed to receive from pipe: " << received.error() << endl;
    return 1;
  }

  Option<Error> result =
    thunkOut<Usage, UsageResult>(directory, received.get());
  if (result.isSome()) {
    cerr << "Usage thunking failed: " << result.get().message << endl;
    return 1;
  }
  return 0;
}


// Receive active containers.
int containers(const string& directory)
{
  // We may be asked for containers even if we never received a
  // launch on this slave.
  if (!os::isfile(pidPath(directory))) {
    // Answer the request with an empty containers message.
    Containers containers;
    Try<Nothing> sent = send(containers);
    if (sent.isError()) {
      cerr << "Failed to send to pipe: " << sent.error() << endl;
    }
    return 0;
  }

  ContainersRequest request;

  Option<Error> result =
    thunkOut<ContainersRequest, ContainersResult>(directory, request);
  if (result.isSome()) {
    cerr << "Containers thunking failed: " << result.get().message << endl;
    return 1;
  }
  return 0;
}


// Terminate the containerized executor.
int destroy(const string& directory)
{
  // Receive the message via pipe.
  Result<Destroy> received = receive<Destroy>();
  if (received.isError()) {
    cerr << "Failed to receive from pipe: " + received.error() << endl;
    return 1;
  }

  Try<PID<ThunkProcess> > pid = thunkPid(directory);
  if (pid.isError()) {
    cerr << "Failed to initialize: " << pid.error() << endl;
    return 1;
  }

  // Destroy does not return a future, hence we can not validate its
  // results, thus we do a simple fire-and-forget post.
  post(pid.get(), received.get());

  return 0;
}


// Recursively run this executable by passing in the 'setup'
// command. stderr output is redirected o a subdirectory of
// the given MESOS_WORK_DIRECTORY into the 'stderr' log file.
// This call blocks until the daemon has fully initialized.
Option<Error> daemonize(const string& argv0, const string& directory)
{
  // Create a named pipe for syncing parent and child process.
  if (mkfifo(fifoPath(directory).c_str(), 0666) < 0) {
    return ErrnoError("Failed to create fifo");
  }

  string command = argv0 + " setup 2>>" + stderrPath(directory);

  int pid = ::fork();
  if (pid == -1) {
    return ErrnoError("Failed to fork");;
  }
  if (pid == 0) {
    execl("/bin/sh", "sh", "-c", command.c_str(), (char*) NULL);
    ABORT("exec failed");
  }

  // We are in the parent context. Sync parent and child process.
  Try<int> pipe = os::open(fifoPath(directory), O_RDONLY);
  if (pipe.isError()) {
    return Error("Failed to open fifo : " + pipe.error());
  }
  int sync;
  while (::read(pipe.get(), &sync, sizeof(sync)) == -1 &&
         errno == EINTR);
  os::close(pipe.get());

  return None();
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


int main(int argc, char** argv)
{
  hashmap<string, int(*)(const string&)> methods;

  // Daemon setup. Invoked by the test-containerizer itself only.
  methods["setup"] = setup;

  // Containerizer specific implementations.
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
    cout << "'" << command << "' is not a valid command" << endl;
    usage(argv[0], methods.keys());
    exit(1);
  }

  CHECK(os::hasenv("MESOS_WORK_DIRECTORY"))
    << "Missing MESOS_WORK_DIRECTORY environment variable";
  string directory = os::getenv("MESOS_WORK_DIRECTORY");

  // Create test-containerizer work directory.
  CHECK_SOME(os::mkdir(thunkDirectory(directory)))
    << "Failed to create test-containerizer work directory '"
    << thunkDirectory(directory) << "'";

  // Implicitly fork-exec the daemon on 'launch' if none is active
  // for the given slave instance, identified via
  // MESOS_WORK_DIRECTORY.
  if (command == "launch") {
    // We need a file lock at this point to prevent double daemonizing
    // attempts due to concurrent launch invocations.
    Try<int> lock = os::open(
        lockPath(directory),
        O_WRONLY | O_CREAT,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if (lock.isError()) {
      cerr << "Failed to create lock file: " << lock.error() << endl;
      exit(1);
    }

    if (::flock(lock.get(), LOCK_EX) != 0) {
      os::close(lock.get());
      cerr << ErrnoError("Failed to lock file").message << endl;
      exit(1);
    }

    // Checks if a daemon is running and if not, it forks one.
    if (!os::isfile(pidPath(directory))) {
      Option<Error> daemonized = daemonize(argv[0], directory);
      if (daemonized.isSome()) {
        cerr << "Daemonizing failed: " << daemonized.get().message << endl;
        if (::flock(lock.get(), LOCK_UN) != 0) {
          cerr << ErrnoError("Could not unlock file").message << endl;
        }
        os::close(lock.get());
        exit(1);
      }
    }

    if (::flock(lock.get(), LOCK_UN) != 0) {
      cerr << ErrnoError("Could not unlock lock file").message << endl;
      os::close(lock.get());
      exit(1);
    }

    os::close(lock.get());
  }

  return methods[command](directory);
}

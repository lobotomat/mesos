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
using namespace mesos::containerizer;
using namespace mesos::containerizer::examples;
using namespace mesos::internal::slave;

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
static Try<T> validate(const Future<T>& future)
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


static string thunkDirectory(const string& directory)
{
  return path::join(directory, "test_containerizer");
}


// Allows you to describe request/response protocols and then use
// those for sending requests and getting back responses.
template <typename Req, typename Res>
struct SyncProtocol
{
  Try<Res> operator () (
      const process::UPID& pid,
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
  explicit ThunkProcess(MesosContainerizerProcess* containerizer)
  : containerizer(containerizer), garbageCollecting(false) {}

  virtual ~ThunkProcess() {}

protected:
  virtual void initialize()
  {
    try {
      std::fstream file(pidPath, std::ios::out);
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
    int pipe = open(fifoPath.c_str(), O_WRONLY);
    if (pipe < 0) {
      cerr << "Failed open fifo: " << strerror(errno) << endl;
      return;
    }
    int sync = 0;
    while (::write(pipe, &sync, sizeof(sync)) == -1 &&
           errno == EINTR);
    close(pipe);
  }

  virtual void finalize()
  {
    Try<Nothing> rmPid = os::rm(pidPath);
    if (rmPid.isError()) {
      cerr << "Failed to remove '" << pidPath << "': " << rmPid.error()
           << endl;
    }
  }

private:
  void shutdown()
  {
    // Join the MesosContainerizer process.
    terminate(containerizer->self());

    // Suicide.
    terminate(this->self());
  }

  void startGarbageCollecting()
  {
    if (garbageCollecting) {
      return;
    }
    garbageCollecting = true;
    garbageCollect();
  }

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

  void _garbageCollect(const hashset<ContainerID>& containers)
  {
    if(containers.empty()) {
      // When no containers are active, shutdown.
      shutdown();
      return;
    }
    // Garbage collect forever.
    process::delay(Seconds(1), self(), &Self::garbageCollect);
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

    // Transmit the result back to the request process.
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

public:
  string fifoPath;
  string pidPath;
  bool garbageCollecting;
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
  TestContainerizer(const string& argv0, const string& directory)
  : argv0(argv0), directory(directory) {}

  virtual ~TestContainerizer() {}

private:
  // Prepare the MesosContainerizerProcess to use posix-isolation,
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

  Option<Error> daemonize()
  {
    string workDirectory = thunkDirectory(directory);

    // Create a named pipe for syncing parent and child process.
    string fifoPath = path::join(workDirectory, "fifo");
    if (mkfifo(fifoPath.c_str(), 0666) < 0) {
      return ErrnoError("Failed to create fifo");
    }

    string daemonStderr = path::join(workDirectory, "daemon_stderr");
    string command = argv0 + " setup 2>>" + daemonStderr;

    // Spawn the process and wait for it in a child process.
    int pid = ::fork();
    if (pid == -1) {
      return ErrnoError("Failed to fork");;
    }
    if (pid == 0) {
      execl("/bin/sh", "sh", "-c", command.c_str(), (char*) NULL);
      ABORT("exec failed");
    }

    // We are in the parent context. Sync parent and child process.
    int pipe = open(fifoPath.c_str(), O_RDONLY);
    if (pipe < 0) {
      return ErrnoError("Failed open fifo");
    }
    int sync;
    while (::read(pipe, &sync, sizeof(sync)) == -1 &&
           errno == EINTR);
    close(pipe);

    return None();
  }

  // Tries to get the PID of a running daemon.
  Try<PID<ThunkProcess> > initialize()
  {
    const string& workDirectory(thunkDirectory(directory));
    // An existing pid-file signals that the daemon is active.
    if (!os::isfile(path::join(workDirectory, "pid"))) {
      return Error("PID-file does not exist");
    }

    PID<ThunkProcess> pid;
    try {
      std::ifstream file(path::join(workDirectory, "pid"));
      file >> pid;
      file.close();
    } catch(std::exception e) {
      return Error(string("Failed reading PID: ") + e.what());
    }

    return pid;
  }

  // Transmit a message via socket to the MesosContainerizer, block
  // until a result message is received.
  template <typename T, typename R>
  Try<R> thunk(const T& t)
  {
    Try<PID<ThunkProcess> > pid = initialize();
    if (pid.isError()) {
      return Error("Failed to initialize: " + pid.error());
    }

    // Send a request and receive an answer via process protobuf
    // exchange.
    SyncProtocol<T, R> protocol;
    return protocol(pid.get(), t);
  }

  // Transmit a message via socket to the MesosContainerizer, block
  // until a result message is received and pipe it out.
  template <typename T, typename R>
  Option<Error> thunkOut(const T& t)
  {
    Try<R> result = thunk<T, R>(t);
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

public:
  int setup()
  {
    const string& workDirectory(thunkDirectory(directory));

    Try<MesosContainerizerProcess*> containerizer =
      createContainerizer(directory);
    if (containerizer.isError()) {
      cerr << "Failed to create MesosContainerizerProcess: "
           << containerizer.error() << endl;
      return 1;
    }

    // Spawn the containerizer process.
    MesosContainerizerProcess* container = containerizer.get();
    process::spawn(container, true);

    // TODO(tillt): move this into the ThunkProcess and hand over
    // the workdir only.

    // Create a named pipe for syncing parent and child process.
    string fifoPath = path::join(workDirectory, "fifo");
    // Serialize the PID to the filesystem.
    string pidPath = path::join(workDirectory, "pid");

    // Create our ThunkProcess, wrapping the containerizer process.
    ThunkProcess* process = new ThunkProcess(container);
    process->fifoPath = fifoPath;
    process->pidPath = pidPath;

    // Run until we get terminated via teardown.
    process::spawn(process, true);
    process::wait(process);

    Try<Nothing> rmFifo = os::rm(fifoPath);
    if (rmFifo.isError()) {
      cerr << "Failed to remove '" << fifoPath << "': " << rmFifo.error()
           << endl;
    }

    os::rmdir(workDirectory);

    return 0;
  }

  // Recover all containerized executors states.
  int recover()
  {
    // This implementation does not persist any states, hence it does
    // need or support an internal recovery.
    return 0;
  }

  // Start a containerized executor. Expects to receive a Launch
  // protobuf via stdin.
  int launch()
  {
    const string& workDirectory(thunkDirectory(directory));

    Result<Launch> received = receive<Launch>();
    if (received.isError()) {
      cerr << "Failed to receive from pipe: " << received.error() << endl;
      return 1;
    }

    // Create test-comtainerizer work directory.
    CHECK_SOME(os::mkdir(workDirectory))
      << "Failed to create test-containerizer work directory '"
      << workDirectory << "'";

    string lockPath = path::join(workDirectory, "pid_lock");
    int fd;

    // We need a file lock at this point to prevent double daemonizing
    // attempts.
    while (true) {
      fd = open(lockPath.c_str(), O_WRONLY | O_CREAT | O_EXLOCK);
      if (fd != -1 || (fd == -1 && (errno != EINTR && errno != EACCES))) {
        break;
      }
      os::sleep(Milliseconds(100));
    }
    if (fd == -1) {
      cerr << "Failed to access lock file " << strerror(errno) << endl;
      return 1;
    }

    // Checks if a daemon is running and if not, it forks one.
    if (!os::isfile(path::join(workDirectory, "pid"))) {
      Option<Error> daemonized = daemonize();
      if (daemonized.isSome()) {
        cerr << "Daemonizing failed: " << daemonized.get().message << endl;
        close(fd);
        os::rm(lockPath);
        return 1;
      }
    }

    close(fd);
    os::rm(lockPath);

    // We need to wrap the Launch message as "install" only supports
    // up to 6 parameters whereas the Launch message has 8 members.
    LaunchRequest wrapped;
    wrapped.mutable_message()->CopyFrom(received.get());

    Try<LaunchResult> result = thunk<LaunchRequest, LaunchResult>(wrapped);
    if (result.isError()) {
      cerr << "Launch thunking failed: " << result.error() << endl;
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

    Option<Error> result = thunkOut<Wait, WaitResult>(received.get());
    if (result.isSome()) {
      cerr << "Wait thunking failed: " << result.get().message << endl;
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

    Try<UpdateResult> result = thunk<Update, UpdateResult>(received.get());
    if (result.isError()) {
      cerr << "Update thunking failed: " << result.error() << endl;
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

    Option<Error> result = thunkOut<Usage, UsageResult>(received.get());
    if (result.isSome()) {
      cerr << "Usage thunking failed: " << result.get().message << endl;
      return 1;
    }
    return 0;
  }

  // Receive active containers.
  int containers()
  {
    // We may be asked for containers even if we never received a
    // launch on this slave.
    const string& workDirectory(thunkDirectory(directory));
    if (!os::isfile(path::join(workDirectory, "pid"))) {
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
      thunkOut<ContainersRequest, ContainersResult>(request);
    if (result.isSome()) {
      cerr << "Containers thunking failed: " << result.get().message << endl;
      return 1;
    }
    return 0;
  }

  // Terminate the containerized executor.
  int destroy()
  {
    // Receive the message via pipe.
    Result<Destroy> received = receive<Destroy>();
    if (received.isError()) {
      cerr << "Failed to receive from pipe: " + received.error() << endl;
      return 1;
    }

    Try<PID<ThunkProcess> > pid = initialize();
    if (pid.isError()) {
      cerr << "Failed to initialize: " << pid.error() << endl;
      return 1;
    }

    // Destroy does not return a future, hence we can not validate its
    // results, thus we do a simple fire-and-forget post.
    process::post(pid.get(), received.get());

    return 0;
  }

private:
  string argv0;
  string directory;
};


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

static bool enableCoreDumps(void)
{
    struct rlimit limit;

    limit.rlim_cur = RLIM_INFINITY;
    limit.rlim_max = RLIM_INFINITY;

    return setrlimit(RLIMIT_CORE, &limit) == 0;
}

int main(int argc, char** argv)
{
  using namespace mesos;
  using namespace internal;
  using namespace slave;

  enableCoreDumps();

  CHECK(os::hasenv("MESOS_WORK_DIRECTORY"))
    << "Missing MESOS_WORK_DIRECTORY environment variable";
  string directory = os::getenv("MESOS_WORK_DIRECTORY");

  hashmap<string, function<int(const string&)> > methods;

  TestContainerizer containerizer(argv[0], directory);

  methods["setup"] = lambda::bind(
      &TestContainerizer::setup, &containerizer);

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

  if (!methods.contains(command)) {
    cout << "'" << command << "' is not a valid command" << endl;
    usage(argv[0], methods.keys());
    exit(1);
  }

  return methods[command](directory);
}

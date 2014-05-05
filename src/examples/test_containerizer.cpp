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
#include <mesos/containerizer/containerizer.hpp>

#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/hashset.hpp>
#include <stout/flags.hpp>
#include <stout/lambda.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>

#include "slave/flags.hpp"

#include "slave/containerizer/mesos_containerizer.hpp"

#include "slave/containerizer/isolators/posix.hpp"

using namespace mesos;
using namespace mesos::containerizer;

using process::Owned;
using process::UPID;
using process::PID;

using std::cout;
using std::cerr;
using std::endl;
using std::ofstream;
using std::ifstream;
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
    : MesosContainerizerProcess(flags, true, launcher, isolators) {}

  virtual ~TestContainerizerProcess() {}

private:
};


class TestContainerizer
{
public:
  TestContainerizer() : path("/tmp/mesos-test-containerizer")
  {
  }

  virtual ~TestContainerizer()
  {
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

    pid = PID<TestContainerizerProcess>(process);

    // Serialize the UPID.
    ofstream file(path::join(path, "pid"));
    file << pid;
    file.close();

    cerr << "PID: " << pid << endl;

    process::Future<Nothing> future = process::dispatch(
        pid, &MesosContainerizerProcess::recover, None());

    cerr << "awaiting recovery" << endl;

    future.await();

    cerr << "recovery done" << endl;

    process::wait(process);
    delete process;

    return 0;
  }

  int teardown()
  {
    ifstream file(path::join(path, "pid"));
    file >> pid;
    file.close();

    cerr << "PID: " << pid << endl;

    process::initialize();

    process::Future<Nothing> future = process::dispatch(
        pid, &MesosContainerizerProcess::recover, None());

    cerr << "awaiting recovery" << endl;

    future.await();

    cerr << "recovery done" << endl;

    process::terminate(pid);
    process::wait(pid);

    return 0;
  }

  void init(const UPID& pid)
  {
    //process
  }

  // Recover all containerized executors states.
  int recover()
  {
    return 0;
  }

  // Start a containerized executor. Expects to receive an Launch
  // protobuf via stdin.
  int launch()
  {
    Result<containerizer::Launch> launch = receive<containerizer::Launch>();
    if (launch.isError()) {
      cerr << "Failed to receive Launch message: " << launch.error() << endl;
      return 1;
    }

    return 0;
  }

  // Get the containerized executor's Termination.
  // Delivers a Termination protobuf filled with the information
  // gathered from launch's wait via stdout.
  int wait()
  {
    Result<containerizer::Wait> wait = receive<containerizer::Wait>();
    if (wait.isError()) {
      cerr << "Failed to receive Wait message: " << wait.error() << endl;
      return 1;
    }

    containerizer::Termination termination;

    Try<Nothing> sent = send(termination);
    if (sent.isError()) {
      cerr << "Failed to send Termination: " << sent.error() << endl;
      return 1;
    }

    return 0;
  }

  // Update the container's resources.
  // Expects to receive a Update protobuf via stdin.
  int update()
  {
    Result<containerizer::Update> update = receive<containerizer::Update>();
    if (update.isError()) {
      cerr << "Failed to receive Update message: " << update.error() << endl;
      return 1;
    }

    return 0;
  }

  // Gather resource usage statistics for the containerized executor.
  // Delivers an ResourceStatistics protobut via stdout when
  // successful.
  int usage()
  {
    Result<containerizer::Usage> usage = receive<containerizer::Usage>();
    if (usage.isError()) {
      cerr << "Failed to receive Usage message: " << usage.error() << endl;
      return 1;
    }

    ResourceStatistics statistics;

    Try<Nothing> sent = send(statistics);
    if (sent.isError()) {
      cerr << "Failed to send ResourceStatistics: " << sent.error() << endl;
      return 1;
    }

    return 0;
  }

  // 
  int containers()
  {
    containerizer::Containers containers;

    Try<Nothing> sent = send(containers);
    if (sent.isError()) {
      cerr << "Failed to send Containers: " << sent.error() << endl;
      return 1;
    }

    return 0;
  }

  // Terminate the containerized executor.
  int destroy()
  {
    Result<containerizer::Destroy> destroy = receive<containerizer::Destroy>();
    if (destroy.isError()) {
      cerr << "Failed to receive Destroy message: " << destroy.error() << endl;
      return 1;
    }

    return 0;
  }

private:
  PID<MesosContainerizerProcess> pid;
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

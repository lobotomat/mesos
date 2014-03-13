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

#include <unistd.h>

#include <gmock/gmock.h>

#include <string>
#include <vector>
#include <map>

#include <mesos/resources.hpp>

#include <process/future.hpp>

#include <stout/os.hpp>
#include <stout/path.hpp>

#include "master/master.hpp"
#include "master/detector.hpp"

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/external_containerizer.hpp"
#include "slave/flags.hpp"
#include "slave/slave.hpp"

#include "tests/mesos.hpp"
#include "tests/flags.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::tests;

using namespace process;

using mesos::internal::master::Master;
using mesos::internal::slave::Containerizer;
using mesos::internal::slave::Slave;

using std::string;
using std::vector;

using testing::_;
using testing::DoAll;
using testing::Return;
using testing::SaveArg;

class ExternalContainerizerTest : public MesosTest {};


class TestExternalContainerizer : public slave::ExternalContainerizer
{
public:
  TestExternalContainerizer(slave::Flags flags)
  : ExternalContainerizer(flags), flags(flags) {}

  Future<ExecutorInfo> launch(
      const ContainerID& containerId,
      const TaskInfo& taskInfo,
      const FrameworkID& frameworkId,
      const string& directory,
      const Option<string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint)
  {
    ExecutorInfo executor = containerExecutorInfo(
        flags, taskInfo, frameworkId);
    executor.mutable_resources()->MergeFrom(taskInfo.resources());

    std::pair<FrameworkID, ExecutorID> key(executor.framework_id(),
                                           executor.executor_id());
    containers[key] = containerId;

    return ExternalContainerizer::launch(
        containerId,
        taskInfo,
        frameworkId,
        directory,
        user,
        slaveId,
        slavePid,
        checkpoint);
  }

  Try<ContainerID> getContainer(
      const FrameworkID& frameworkId,
      const ExecutorID& executorId)
  {
    std::pair<FrameworkID, ExecutorID> key(frameworkId, executorId);
    if (!containers.contains(key)) {
      return Error(
          "Container for executor '" + stringify(executorId) +
          "' of framework '" + stringify(frameworkId) + "' not found");
    }
    return containers[key];
  }

private:
  hashmap<std::pair<FrameworkID, ExecutorID>, ContainerID> containers;
  const slave::Flags flags;
};


TEST_F(ExternalContainerizerTest, Launch)
{
  Try<PID<Master> > master = this->StartMaster();
  ASSERT_SOME(master);

  Flags testFlags;

  slave::Flags flags = this->CreateSlaveFlags();

  flags.isolation = "external";
  flags.containerizer_path =
      string(testFlags.build_dir) + "/src/examples/python/test-containerizer";

  TestExternalContainerizer testContainerizer(flags);

  Try<PID<Slave> > slave = this->StartSlave(&testContainerizer, flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get(), DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer> > offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(frameworkId);
  AWAIT_READY(offers);

  EXPECT_NE(0u, offers.get().size());

  TaskInfo task;
  task.set_name("isolator_test");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(offers.get()[0].resources());

  Resources resources(offers.get()[0].resources());
  Option<Bytes> mem = resources.mem();
  ASSERT_SOME(mem);
  Option<double> cpus = resources.cpus();
  ASSERT_SOME(cpus);

  const std::string& file = path::join(flags.work_dir, "ready");

  // This task induces user/system load in a child process by
  // running top in a child process for ten seconds.
  task.mutable_command()->set_value(
#ifdef __APPLE__
      // Use logging mode with 30,000 samples with no interval.
      "top -l 30000 -s 0 2>&1 > /dev/null & "
#else
      // Batch mode, with 30,000 samples with no interval.
      "top -b -d 0 -n 30000 2>&1 > /dev/null & "
#endif
      "touch " + file +  "; " // Signals that the top command is running.
      "sleep 60");

  vector<TaskInfo> tasks;
  tasks.push_back(task);

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status))
    .WillRepeatedly(Return()); // Ignore rest for now

  driver.launchTasks(offers.get()[0].id(), tasks);

  AWAIT_READY(status);

  EXPECT_EQ(TASK_RUNNING, status.get().state());

  // Wait for the task to begin inducing cpu time.
  while (!os::exists(file));

  ExecutorID executorId;
  executorId.set_value(task.task_id().value());

  // We'll wait up to 10 seconds for the child process to induce
  // 1/8 of a second of user and system cpu time in total.
  // TODO(bmahler): Also induce rss memory consumption, by re-using
  // the balloon framework.
  ResourceStatistics statistics;
  Duration waited = Duration::zero();
  do {
    Try<ContainerID> containerId =
        testContainerizer.getContainer(frameworkId.get(), executorId);
    EXPECT_SOME(containerId);

    Future<ResourceStatistics> usage = testContainerizer.usage(
        containerId.get());
    AWAIT_READY(usage);

    statistics = usage.get();

    // If we meet our usage expectations, we're done!
    if (statistics.cpus_user_time_secs() >= 0.120 &&
        statistics.cpus_system_time_secs() >= 0.05 &&
        statistics.mem_rss_bytes() >= 1024u) {
      break;
    }

    os::sleep(Milliseconds(100));
    waited += Milliseconds(100);
  } while (waited < Seconds(10));

  EXPECT_GE(statistics.cpus_user_time_secs(), 0.120);
  EXPECT_GE(statistics.cpus_system_time_secs(), 0.05);
  EXPECT_EQ(statistics.cpus_limit(), cpus.get());
  EXPECT_GE(statistics.mem_rss_bytes(), 1024u);
  EXPECT_EQ(statistics.mem_limit_bytes(), mem.get().bytes());

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.killTask(task.task_id());

  AWAIT_READY(status);

  EXPECT_EQ(TASK_KILLED, status.get().state());

  driver.stop();
  driver.join();

  this->Shutdown();
}

// TODO(nnielsen): Add tests addressing executor/container matrix.
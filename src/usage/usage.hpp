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

#ifndef __USAGE_HPP__
#define __USAGE_HPP__

#include <unistd.h> // For pid_t.

#include <deque>

#include <mesos/mesos.hpp>

#include <process/clock.hpp>

#include <stout/foreach.hpp>
#include <stout/os.hpp>

namespace mesos {
namespace internal {

// Collects resource usage of a process tree rooted at 'pid'. Only
// collects the 'mem_*' values if 'mem' is true and the 'cpus_*'
// values if 'cpus' is true.
ResourceStatistics usage(pid_t pid, bool mem = true, bool cpus = true)
{
  Try<os::ProcessTree> pstree = os::pstree(pid);

  if (pstree.isError()) {
    return ResourceStatistics();
  }

  ResourceStatistics statistics;

  // The timestamp is the only required field.
  statistics.set_timestamp(process::Clock::now().secs());

  std::deque<os::ProcessTree> trees;
  trees.push_back(pstree.get());

  while (!trees.empty()) {
    const os::ProcessTree& tree = trees.front();

    if (mem) {
      if (tree.process.rss.isSome()) {
        statistics.set_mem_rss_bytes(
            statistics.mem_rss_bytes() + tree.process.rss.get().bytes());
      }
    }

    // We only show utime and stime when both are available, otherwise
    // we're exposing a partial view of the CPU times.
    if (cpus) {
      if (tree.process.utime.isSome() && tree.process.stime.isSome()) {
        statistics.set_cpus_user_time_secs(
            statistics.cpus_user_time_secs() +
            tree.process.utime.get().secs());

        statistics.set_cpus_system_time_secs(
            statistics.cpus_system_time_secs() +
            tree.process.stime.get().secs());
      }
    }

    foreach (const os::ProcessTree& child, tree.children) {
      trees.push_back(child);
    }

    trees.pop_front();
  }

  return statistics;
}

} // namespace internal {
} // namespace mesos {

#endif // __USAGE_HPP__

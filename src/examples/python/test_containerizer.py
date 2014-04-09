#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The scheme an external containerizer has to adhere to is;
#
# COMMAND (ADDITIONAL-PARAMETERS) < INPUT-PROTO > RESULT-PROTO
#
# launch < Launch
# update < Update
# usage < ContainerID > ResourceStatistics
# wait < ContainerID > Termination
# destroy < ContainerID
#

import fcntl
import multiprocessing
import os
import subprocess
import sys
import struct
import time

# Render a string describing how to use this script.
def use(container, methods):
    out = "Usage: %s <command>\n" % container
    out += "Valid commands: " + ', '.join(methods.keys())

    return out


# Read a data chunk prefixed by its total size from stdin.
def receive():
    # Read size (uint32 => 4 bytes).
    size = struct.unpack('I', sys.stdin.read(4))
    if size[0] <= 0:
        print >> sys.stderr, "Expected protobuf size over stdin. " \
                             "Received 0 bytes."
        return ""

    # Read payload.
    data = sys.stdin.read(size[0])
    if len(data) != size[0]:
        print >> sys.stderr, "Expected %d bytes protobuf over stdin. " \
                             "Received %d bytes." % (size[0], len(data))
        return ""

    return data


# Write a protobuf message prefixed by its total size (aka recordio)
# to stdout.
def send(data):
    # Write size (uint32 => 4 bytes).
    os.write(1, struct.pack('I', len(data)))

    # Write payload.
    os.write(1, data)


# Start a containerized executor.
# Expects to receive an ExternalTask protobuf via stdin and will deliver
# an ExternalStatus protobuf via stdout when successful.
def launch():
    try:
        data = receive()
        if len(data) == 0:
            return 1

        launch = containerizer_pb2.Launch()
        launch.ParseFromString(data)

        if launch.task.HasField("executor"):
            command = ["sh",
                       "-c",
                       launch.task.executor.command.value]
        else:
            print >> sys.stderr, "No executor passed; using mesos_executor!"
            command = [launch.mesos_executor_path,
                       "sh",
                       "-c",
                       launch.task.command.value]

            lock_dir = "/tmp/mesos-test-containerizer"
            subprocess.check_call(["mkdir", "-p", lock_dir])

            lock = os.path.join(lock_dir, launch.container_id.value)

            with open(lock, "w+") as lk:
                fcntl.flock(lk, fcntl.LOCK_EX)
                pid = os.fork()
                if pid == 0:
                    # We are in the child.
                    proc = subprocess.Popen(command, env=os.environ.copy())

                    returncode = proc.wait()
                    lk.write(str(returncode) + "\n")
                    sys.exit(returncode)

    except google.protobuf.message.DecodeError:
        print >> sys.stderr, "Could not deserialise Launch protobuf"
        return 1

    except OSError as e:
        print >> sys.stderr, e.strerror
        return 1

    except ValueError:
        print >> sys.stderr, "Value is invalid"
        return 1

    return 0


# Update the container's resources.
# Expects to receive a ResourceArray protobuf via stdin and will
# deliver an ExternalStatus protobuf via stdout when successful.
def update():
    try:
        data = receive()
        if len(data) == 0:
            return 1

        update = containerizer_pb2.Update()
        update.ParseFromString(data)

        print >> sys.stderr, "Received "                \
                           + str(len(update.resources)) \
                           + " resource elements."

    except google.protobuf.message.DecodeError:
        print >> sys.stderr, "Could not deserialise Update protobuf."
        return 1

    except OSError as e:
        print >> sys.stderr, e.strerror
        return 1

    except ValueError:
        print >> sys.stderr, "Value is invalid"
        return 1

    return 0


# Gather resource usage statistics for the containerized executor.
# Delivers an ResourceStatistics protobut via stdout when
# successful.
def usage():
    try:
        data = receive()
        if len(data) == 0:
            return 1
        containerId = mesos_pb2.ContainerID()
        containerId.ParseFromString(data)

        statistics = mesos_pb2.ResourceStatistics()

        statistics.timestamp = time.time()

        # Cook up some fake data.
        statistics.mem_rss_bytes = 1073741824
        statistics.mem_limit_bytes = 1073741824
        statistics.cpus_limit = 2
        statistics.cpus_user_time_secs = 0.12
        statistics.cpus_system_time_secs = 0.5

        send(statistics.SerializeToString())

    except google.protobuf.message.DecodeError:
        print >> sys.stderr, "Could not deserialise ContainerID protobuf."
        return 1

    except google.protobuf.message.EncodeError:
        print >> sys.stderr, "Could not serialise ResourceStatistics protobuf."
        return 1

    except OSError as e:
        print >> sys.stderr, e.strerror
        return 1

    return 0


# Terminate the containerized executor.
def destroy():
    try:
        data = receive()
        if len(data) == 0:
            return 1
        containerId = mesos_pb2.ContainerID()
        containerId.ParseFromString(data)

    except google.protobuf.message.DecodeError:
        print >> sys.stderr, "Could not deserialise ContainerID protobuf."
        return 1

    except OSError as e:
        print >> sys.stderr, e.strerror
        return 1

    return 0


# Recover the containerized executor.
def recover():
    try:
        data = receive()
        if len(data) == 0:
            return 1
        containerId = mesos_pb2.ContainerID()
        containerId.ParseFromString(data)

    except google.protobuf.message.DecodeError:
        print >> sys.stderr, "Could not deserialise ContainerID protobuf."
        return 1

    except OSError as e:
        print >> sys.stderr, e.strerror
        return 1

    return 0


# Get the containerized executor's Termination.
# Delivers a Termination protobuf filled with the information
# gathered from launch's wait via stdout.
def wait():
    try:
        data = receive()
        if len(data) == 0:
            return 1
        containerId = mesos_pb2.ContainerID()
        containerId.ParseFromString(data)

        lock_dir = "/tmp/mesos-test-containerizer"
        lock = os.path.join(lock_dir, containerId.value)

        # Obtain shared lock and read exit code from file.
        with open(lock, "r") as lk:
            fcntl.flock(lk, fcntl.LOCK_SH)
        status = int(lk.read())

        # Deliver the termination protobuf back to the slave.
        termination = mesos_pb2.Termination()
        termination.killed = false
        termination.status = status
        termination.message = ""

        send(termination.SerializeToString())

    except google.protobuf.message.DecodeError:
        print >> sys.stderr, "Could not deserialise ContainerID protobuf."
        return 1

    except google.protobuf.message.EncodeError:
        print >> sys.stderr, "Could not serialise Termination protobuf."
        return 1

    except OSError as e:
        print >> sys.stderr, e.strerror
        return 1

    return 1


if __name__ == "__main__":
    methods = { "launch":  launch,
                "update":  update,
                "destroy": destroy,
                "recover": recover,
                "usage":   usage,
                "wait":    wait }

    if sys.argv[1:2] == ["--help"] or sys.argv[1:2] == ["-h"]:
        print use(sys.argv[0], methods)
        sys.exit(0)

    if len(sys.argv) < 3:
        print >> sys.stderr, "Please pass a command"
        print >> sys.stderr, use(sys.argv[0], methods)
        sys.exit(1)

    command = sys.argv[1]
    if command not in methods:
        print >> sys.stderr, "No valid command passed"
        print >> sys.stderr, use(sys.argv[0], methods)
        sys.exit(2)

    method = methods.get(command)

    import mesos
    import mesos_pb2
    import containerizer_pb2
    import google

    sys.exit(method())

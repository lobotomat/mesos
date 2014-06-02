---
layout: documentation
---

# External Containerizer


First some abbreviations used throughout this document:

EC = external containerizer = part of mesos that provides an API for
containerizing via external plugin executables.
ECP = external containerizer program = external plugin executable
implementing the actual containerizing.


# General Overview

EC invokes ECP as a shell process, passing the command as a parameter
to the ECP executable. Many invocations on the ECP will also pass a
protobuf message along via stdin. Some invocations on the ECP also
expect to deliver a result protobuf message back via stdout.
All protobuf messages are prepended by their original length -
this is sometimes referred to as “Record-IO”-format.

Record-IO: `<32bit int = length><binary protobuf data>`

Here comes a quick example on how to send and receive such record-io
formatted message using Python
(taken from src/examples/python/test_containerizer.py):

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

        return data`

    # Write a protobuf message prefixed by its total size (aka recordio)
    # to stdout.
    def send(data):
        # Write size (uint32 => 4 bytes).
        sys.stdout.write(struct.pack('I', len(data)))

        # Write payload.
        sys.stdout.write(data)`

The ECP is expected to return a zero exit code for all commands it
was able to process. A non zero status code signals an error. Below
you will find an overview on commands and their invocation scheme that
has to be implemented within an ECP.


### Call and communication scheme

**COMMAND < INPUT-PROTO > RESULT-PROTO**

* launch < containerizer::Launch
* update < containerizer::Update
* usage < containerizer::Usage > mesos::ResourceStatistics
* wait < containerizer::Wait > containerizer::Termination
* destroy < containerizer::Destroy
* containers > containerizer::Containers
* recover



# Command Details

## launch
### Start the containerized executor
Hands over all information the ECP needs for launching a task
via an executor.
This call should not wait for the executor / command to return. The
actual reaping of the containerized command is done via the `wait`
call.

`launch < containerizer::Launch`

This call receives the containerizer::Launch protobuf via stdin;

    /**
     * Encodes the launch command sent to the external containerizer
     * program.
     */
    message Launch {
      required ContainerID container_id = 1;
      optional TaskInfo task_info = 2;
      optional ExecutorInfo executor_info = 3;
      optional string directory = 4;
      optional string user = 5;
      optional SlaveID slave_id = 6;
      optional string slave_pid = 7;
      optional bool checkpoint = 8;
    }

This call does not return any data via stdout.

## wait
### Gets information on the containerized executor's Termination
Is expected to reap the executor / command. This call should block
until the executor/command has terminated.

`wait < containerizer::Wait > containerizer::Termination`

This call receives the containerizer::Wait protobuf via stdin;

    /**
     * Encodes the wait command sent to the external containerizer
     * program.
     */
    message Wait {
      required ContainerID container_id = 1;
    }

This call is expected to return containerizer::Termination via stdout;

    /**
     * Information about a container termination, returned by the
     * containerizer to the slave.
     */
    message Termination {
      // A container may be killed if it exceeds its resources; this will
      // be indicated by killed=true and described by the message string.
      required bool killed = 1;
      required string message = 2;

      // Exit status of the process.
      optional int32 status = 3;
    }

The Termination attribute `killed` is to be set only when the
containerizer or the underlying isolation had to enforce a limitation
by killing the task (e.g. task exceeded suggested memory limit).

## update
### Updates the container's resource limits
Is sending (new) resource constraints for the given container.
Resource constraints onto a container may vary over the lifetime of
the containerized task.

`update < containerizer::Update`

This call receives the containerizer::Update protobuf via stdin;

    /**
     * Encodes the update command sent to the external containerizer
     * program.
     */
    message Update {
      required ContainerID container_id = 1;
      repeated Resource resources = 2;
    }

This call does not return any data via stdout.

## usage
### Gathers resource usage statistics for a containerized task
Is used for polling the current resource uses for the given container.

`usage < containerizer::Usage > mesos::ResourceStatistics`

This call received the containerizer::Usage protobuf via stdin;

    /**
     * Encodes the usage command sent to the external containerizer
     * program.
     */
    message Usage {
      required ContainerID container_id = 1;
    }

This call is expected to return mesos::ResourceStatistics via stdout;

    /*
     * A snapshot of resource usage statistics.
     */
    message ResourceStatistics {
      required double timestamp = 1; // Snapshot time, in seconds since the Epoch.

      // CPU Usage Information:
      // Total CPU time spent in user mode, and kernel mode.
      optional double cpus_user_time_secs = 2;
      optional double cpus_system_time_secs = 3;

      // Number of CPUs allocated.
      optional double cpus_limit = 4;

      // cpu.stat on process throttling (for contention issues).
      optional uint32 cpus_nr_periods = 7;
      optional uint32 cpus_nr_throttled = 8;
      optional double cpus_throttled_time_secs = 9;

      // Memory Usage Information:
      optional uint64 mem_rss_bytes = 5; // Resident Set Size.

      // Amount of memory resources allocated.
      optional uint64 mem_limit_bytes = 6;

      // Broken out memory usage information (files, anonymous, and mmaped files)
      optional uint64 mem_file_bytes = 10;
      optional uint64 mem_anon_bytes = 11;
      optional uint64 mem_mapped_file_bytes = 12;
    }

## destroy
### Terminates the containerized executor
Is used in rare situations, like for graceful slave shutdown
but also in slave failover scenarios - see Slave Recovery for more.

`destroy < containerizer::Destroy`

This call receives the containerizer::Destroy protobuf via stdin;

    /**
     * Encodes the destroy command sent to the external containerizer
     * program.
     */
    message Destroy {
      required ContainerID container_id = 1;
    }

This call does not return any data via stdout.

## containers
### Gets all active container-id's
Returns all container identifiers known to be currently active.

`containers > containerizer::Containers`

This call does not receive any additional data via stdin.

This call is expected to pass containerizer::Containers back via stdout;

    /**
     * Information on all active containers returned by the containerizer
     * to the slave.
     */
    message Containers {
      repeated ContainerID containers = 1;
    }


## recover
### Internal ECP state recovery
Allows the ECP to do a state recovery on its own. If the ECP
uses state checkpointing e.g. via filesystem, then this call would be
a good moment to deserialize that state information. Make sure you
also see Slave Recovery below for more.

`recover`

This call does not receive any additional data via stdin.
No returned data via stdout.



### Protobuf Message Definitions

For possibly more up-to-date versions of the above mentioned protobufs
as well as protobuf messages referenced by them, please check:
containerizer::XXX are defined within
include/mesos/containerizer/containerizer.proto.
mesos::XXX are defined within include/mesos/mesos.proto.


## Addional Environment Variables

Additionally, there are a few new environment variables set when
invoking the ECP.


* MESOS_LIBEXEC_DIRECTORY = path to mesos-executor, mesos-usage, ...

* MESOS_WORK_DIRECTORY = slave work directory. This should be used for
distiguishing slave instances.

**Note** that this is specifically helpful for being able to tie a set
of containers to a specific slave instance, thus allowing proper
recovery when needed.

* MESOS_DEFAULT_CONTAINER_IMAGE = default image as provided via slave
flags (default_container_image). This variable is provided only in
calls to 'launch'.



## Example Scenarios


# Task Launching

* EC invokes ‘launch’ on the ECP.
 * Along with that call, the ECP will receive a containerizer::Launch
 protobuf message via stdin.
 * ECP now makes sure the executor gets started.
**Note** that ‘launch’ is not supposed to block. It should return
immediately after triggering the executor/command - that could be done
via fork-exec within the ECP.
* EC invokes ‘wait' on the ECP.
 * Along with that call, the ECP will receive a containerizer::Wait
 protobuf message via stdin.
 * ECP now blocks until the launched command is reaped - that could be
 implemented via waitpid within the ECP.
 * Once the command is reaped, the ECP should deliver a
 containerizer::Termination protobuf message via stdout, back to the
 EC.


# Slave Recovery

* Slave recovers via check pointed state.
* EC invokes ‘recover’ on the ECP - there is no protobuf message sent
or expected as a result from this command.
* The ECP may try to recover internal states via its own failover
mechanisms, if needed.
* After ‘recover’ returns, the EC will invoke ‘containers’ on the ECP.
* The ECP should return Containers which is a list of currently active
containers.
**Note** these containers are known to the ECP but might in fact
partially be unknown to the slave (e.g. slave failed after launch but
before or within wait) - those containers are considered to be
orphans.
* The EC now compares the list of slave known containers to those
listed within ‘Containers’. For each orphan it identifies, the slave
will invoke a ‘wait” followed by a ‘destroy’ on the ECP for those
containers.
* Slave will now call ‘wait’ on the ECP (via EC) for all recovered
containers. This does once again put ‘wait' into the position of the
ultimate command reaper.

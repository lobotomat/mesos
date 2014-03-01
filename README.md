# Mesos Xcode Project

For fun and profit!

## Preconditions

You still need to full build-setup ready that is used for a "regular" Mesos build, without the Xcode IDE. For details on these first steps, please consult docs/getting-started.md.

Your first build has to be done using the commandline make as that will trigger further configuration,extraction and patching of the 3rdparty dependencies (e.g. libev etc.).

This Xcode build configuration, for reasons of simplification and speedup, uses prebuilt and installed versions of glog, protobuf and leveldb. Make sure those are installed using homebrew (or MacPorts if you prefer that).


## Using the Mesos Xcode Workspace

1. Install availalble 3rdparty dependencies via Homebrew

	Install glog

	`$ brew install glog`

	Install protobuf

	`$ brew install protobuf`

	Install leveldb

	`$ brew install leveldb`

2. Building Mesos for the first time

	Change working directory

	`$ cd mesos`

	Bootstrap

	`$ ./bootstrap`

	Configure and build

	`$ mkdir build`
	`$ cd build`
	`$ ../configure`
	`$ make -j`

3. Building Mesos using the Xcode IDE

	Open the Mesos Workspace

	`$ open contrib/mesos.workspace`

	Now select the mesos-all target:

	![Select Scheme](https://github.com/lobotomat/mesos/blob/master/docs/images/xcode-select-scheme.png)

	![All Scheme](https://github.com/lobotomat/mesos/blob/master/docs/images/xcode-all-scheme.png)

	The above scheme builds libprocess, libmesos_no_3rdparty as well as all mesos runnables.


You may now continue editing, running, debugging and maybe even profiling (Instruments, **yes**). Enjoy!


# About Apache Mesos

Apache Mesos is a cluster manager that provides efficient resource isolation 
and sharing across distributed applications, or frameworks. It can run Hadoop, 
MPI, Hypertable, Spark, and other frameworks on a dynamically shared pool of 
nodes.

Visit us at [mesos.apache.org](http://mesos.apache.org).

# Mailing Lists

 * [User Mailing List](mailto:user-subscribe@mesos.apache.org) ([Archive](https://mail-archives.apache.org/mod_mbox/mesos-user/))
 * [Development Mailing List](mailto:dev-subscribe@mesos.apache.org) ([Archive](https://mail-archives.apache.org/mod_mbox/mesos-dev/))

# Documentation

Documentation is available in the docs/ directory. Additionally, a rendered HTML 
version can be found on the Mesos website's [Documentation](http://mesos.apache.org/documentation/) page.

# Installation

Instructions are included on the [Getting Started](http://mesos.apache.org/gettingstarted/) page.

# License

Apache Mesos is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

For additional information, see the LICENSE and NOTICE files.
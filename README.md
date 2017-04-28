**Note:** This project is no longer actively maintained by Pinterest.

---

Quasar Thrift
=============

[Quasar](http://docs.paralleluniverse.co/quasar/) is a library that provides scalable, lightweight
threads for the JVM (see references below for links to more information).
[Thrift](https://thrift.apache.org) is a code generator for services that works across multiple
languages. This package integrates Quasar with Thrift, providing a high-performance Thrift server
that takes advantage of Quasar's elegant programming model.

## Getting Started

Quasar requires some setup to use correctly, as it uses byte-code instrumentation to implement
lightweight threads. Specifically, Thrift servers using Quasar need to mark some methods as
"suspendable", both in Java code and in separate resource files, and need to use a Java agent. The
best way to get both of these is to copy one of the example projects in the `examples` directory
of this repository. To run the examples, first compile and install the quasar-thrift project:

```./gradlew clean install```

That will install the Maven dependency in your local ~/.m2 directory, where it can be used by the
example projects. Then cd into one of the example project directories and run this command in one
terminal:

```./gradlew clean runServer```

Then, in another terminal, in the same directory, run this command:

```./gradlew runClient```
 
At this time, the easiest way to use Quasar is with Ant or Gradle, as there is an Ant task that
needs to be run after compilation. The `build.gradle` files used by the example servers has setup
that looks like this:

```
classes {
    doFirst {
        ant.taskdef(name: 'scanSuspendables',
                classname: 'co.paralleluniverse.fibers.instrument.SuspendablesScanner',
                classpath: "build/classes/main:build/resources/main:${configurations.runtime.asPath}")
        ant.scanSuspendables(
                auto: true,
                suspendablesFile: "$sourceSets.main.output.resourcesDir/META-INF/suspendables",
                supersFile: "$sourceSets.main.output.resourcesDir/META-INF/suspendable-supers",
                append: true) {
            fileset(dir: sourceSets.main.output.classesDir)
        }
    }
}
```

This tells Quasar to find all the methods to be instrumented at compile time, and to put them into
files named `suspendables` and `suspendable-supers` in the resources output directory. In some
cases you will need to manually mark files for instrumentation as well. Both of the example servers
have done this, and you can see the results in `src/resources/META-INF/suspendables` in both of
the project directories.

If you are getting strange exceptions, typically `NullPointerException`, you may have missed some
methods for instrumentation. At that point, uncomment this line in the build.grade file to have
Quasar give you a better runtime error:

```
    //systemProperties "co.paralleluniverse.fibers.verifyInstrumentation": "true"
```

This will seriously degrade the performance of the server, so make sure to comment it back out
when you are done.

## Architecture

This package provides a Thrift `TServer` that works similarly with all the existing Thrift TServers,
and supports all the available Thrift Protocols and (non-server) Transports. It will work with any
existing Thrift client, in any language (assuming they use the same protocols and transports, of
course, as is true of any Thrift server and client).

This server uses N + 1 Quasar Fibers. A single Fiber is used to accept new connections from clients.
When a connection is accepted, a new Fiber is spawed to manage that connection, reading requests,
processing them, writing responses and then repeating until the connection is closed by the client
or an error. Methods called by the service must either throw `SuspendExecution` or have the
`@Suspendable` annotation in order for Quasar to find them and instrument them. That may not be
necessary in future versions of Quasar.

## Performance

We have only run a small, simple load test of this library. We ran the example "echo" server on an
EC2 m3.2xlarge server with Ubuntu 12.04 and some basic Linux TCP tuning for throughput. Then, on a
separate EC2 instance in the same AZ we ran a load test written using
[Bender](http://github.com/Pinterest/bender). Quasar/Thrift was able to support 6,000 QPS before the
99.9th percentile latency started to increase. We continued the test up to 10,000 QPS at which point
the latency and error rate were relatively high (1% errors, >1s 50th percentile latency). A similar
test using Twitter's Finagle libraries produced roughly the same results up to 7,000 QPS, and better
results after.

## References

The ParallelUniverse blog is a good source of information about Quasar:

* http://blog.paralleluniverse.co/2014/02/06/fibers-threads-strands/
* http://blog.paralleluniverse.co/2014/05/01/modern-java/

## Copyright

Copyright 2014 Pinterest.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Attribution

This library includes open source from the following sources:

* Apache Thrift Libraries Copyright 2014 Apache Software Foundation. Licensed under the Apache License v2.0 (http://www.apache.org/licenses/).
* Quasar Libraries Copyright 2014 Parallel Universe. Licensed under the GNU Lesser General Public License (http://www.gnu.org/licenses/lgpl.html).

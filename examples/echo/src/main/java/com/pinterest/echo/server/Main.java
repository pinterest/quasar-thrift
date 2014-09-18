/*
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
 */
package com.pinterest.echo.server;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.SuspendableRunnable;
import com.pinterest.echo.thrift.EchoService;
import com.pinterest.quasar.thrift.TFiberServer;
import com.pinterest.quasar.thrift.TFiberServerSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFastFramedTransport;

import java.net.InetSocketAddress;

public class Main {
  @Suspendable
  public static void main(String[] args) throws Exception {
    EchoService.Processor<EchoService.Iface> processor =
        new EchoService.Processor<EchoService.Iface>(new EchoServiceImpl());
    TFiberServerSocket trans = new TFiberServerSocket(new InetSocketAddress(9999));
    TFiberServer.Args targs = new TFiberServer.Args(trans, processor)
        .protocolFactory(new TBinaryProtocol.Factory())
        .transportFactory(new TFastFramedTransport.Factory());
    TFiberServer server = new TFiberServer(targs);
    server.serve();
    server.join();
  }
}

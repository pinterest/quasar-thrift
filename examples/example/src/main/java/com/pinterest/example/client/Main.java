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
package com.pinterest.example.client;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.SuspendableRunnable;
import com.pinterest.example.thrift.ExampleEnum;
import com.pinterest.example.thrift.ExampleRequest;
import com.pinterest.example.thrift.ExampleResponse;
import com.pinterest.example.thrift.ExampleService;
import com.pinterest.quasar.thrift.TFiberSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;

import java.net.InetSocketAddress;

public class Main {
  public static void main(String[] args) throws Exception {
    Fiber<Void> f = new Fiber<Void>(new SuspendableRunnable() {
      @Override
      public void run() throws SuspendExecution, InterruptedException {
        try {
          TProtocol protocol = new TBinaryProtocol(new TFastFramedTransport(TFiberSocket.open(new InetSocketAddress(9999))));
          ExampleService.Client client = new ExampleService.Client(protocol);
          ExampleResponse response = client.example(new ExampleRequest().setExampleEnum(ExampleEnum.VALUE_ONE));
          System.out.println(response);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    }).start();
    f.join();
  }
}

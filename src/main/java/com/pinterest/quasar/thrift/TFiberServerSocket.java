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
package com.pinterest.quasar.thrift;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.fibers.io.FiberServerSocketChannel;
import co.paralleluniverse.fibers.io.FiberSocketChannel;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.util.concurrent.TimeUnit;

/**
 * A server transport that uses Fiber-blocking network calls.
 *
 * This class is thread-safe.
 */
public class TFiberServerSocket extends TServerTransport {
  /**
   * Constructor
   *
   * @param addr the port to which the server socket will be bound.
   * @throws TTransportException if the server socket cannot be created.
   */
  public TFiberServerSocket(InetSocketAddress addr) throws TTransportException {
    this.addr = addr;
  }

  /**
   * Binds the server socket to the requested port. This must be called before the server socket
   * can be used to accept incoming requests.
   *
   * The server socket enables SO_REUSEADDR to make it faster to restart a server on the same port.
   *
   * @throws TTransportException if the bind operation fails.
   */
  @Override
  @Suspendable
  public void listen() throws TTransportException {
    try {
      serverSocketChannel = FiberServerSocketChannel.open()
          .setOption(StandardSocketOptions.SO_REUSEADDR, true)
          .bind(addr);
    } catch (IOException ioex) {
      throw new TTransportException(TTransportException.UNKNOWN, ioex);
    }
  }

  /**
   * Closes the server socket.
   */
  @Override
  @Suspendable
  public void close() {
    if (serverSocketChannel != null) {
      try {
        serverSocketChannel.close();
      } catch (IOException ioex) {
        LOG.warn("Failed to close server socket channel", ioex);
      }
    }
  }

  @Override
  @Suspendable
  protected TTransport acceptImpl() throws TTransportException {
    try {
      FiberSocketChannel socketChannel = serverSocketChannel.accept();
      return new TFiberSocket(socketChannel, -1, TimeUnit.SECONDS);
    } catch (SuspendExecution ex) {
      throw new AssertionError("Instrumentation should have removed this code");
    } catch (IOException ioex) {
      throw new TTransportException(TTransportException.UNKNOWN, ioex);
    }
  }

  private volatile FiberServerSocketChannel serverSocketChannel;
  private final InetSocketAddress addr;

  private static final Logger LOG = LoggerFactory.getLogger(TFiberServerSocket.class);
}

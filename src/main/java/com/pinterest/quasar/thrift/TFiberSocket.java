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
import co.paralleluniverse.fibers.io.FiberSocketChannel;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;

/**
 * A Thrift socket that uses Fiber-blocking network calls.
 *
 * This class is thread-safe.
 */
public class TFiberSocket extends TTransport {
  TFiberSocket(FiberSocketChannel socketChannelArg) {
    socketChannel = socketChannelArg;
  }

  public static TFiberSocket open(SocketAddress addr) throws IOException, SuspendExecution {
    FiberSocketChannel fsc = FiberSocketChannel.open(addr);
    return new TFiberSocket(fsc);
  }

  /**
   * Checks that the underlying network connection is open.
   * @return true if the underlying network connection is open.
   */
  @Override
  @Suspendable
  public boolean isOpen() {
    return socketChannel.isOpen();
  }

  /**
   * Open currently does nothing. Socket connection is handled by the static open method.
   */
  @Override
  public void open() throws TTransportException {}

  /**
   * Closes the underlying network connection.
   */
  @Override
  @Suspendable
  public void close() {
    try {
      socketChannel.close();
    } catch (IOException ioex) {
      LOG.warn("Failed to close socket channel", ioex);
    }
  }

  /**
   * Reads up to limit bytes from the underlying socket into the bytes buffer starting at offset.
   *
   * @param bytes must be at least offset + bytes in size.
   * @param offset the offset at which to start writing into bytes.
   * @param limit the maximum number of bytes to read into bytes.
   * @return the number of bytes actually read from the underlying socket.
   * @throws TTransportException if an error occurred while reading.
   */
  @Override
  @Suspendable
  public int read(byte[] bytes, int offset, int limit) throws TTransportException {
    ByteBuffer buf = ByteBuffer.wrap(bytes, offset, limit);

    int bytesRead;
    try {
      bytesRead = socketChannel.read(buf);
    } catch (IOException ioex) {
      throw new TTransportException(TTransportException.UNKNOWN, ioex);
    }

    if (bytesRead < 0) {
      throw new TTransportException(TTransportException.END_OF_FILE);
    }

    return bytesRead;
  }

  /**
   * Writes up to limit bytes starting at offset from the bytes array into the underlying socket.
   * @param bytes the bytes to be written to the underlying socket.
   * @param offset the offset at which to start reading from bytes.
   * @param limit the number of bytes to read from bytes and write to the underlying socket.
   * @throws TTransportException
   */
  @Override
  @Suspendable
  public void write(byte[] bytes, int offset, int limit) throws TTransportException {
    ByteBuffer buf = ByteBuffer.wrap(bytes, offset, limit);
    try {
      socketChannel.write(buf);
    } catch (IOException ioex) {
      throw new TTransportException(TTransportException.UNKNOWN, ioex);
    }
  }

  private final FiberSocketChannel socketChannel;

  private static final Logger LOG = LoggerFactory.getLogger(TFiberSocket.class);
}

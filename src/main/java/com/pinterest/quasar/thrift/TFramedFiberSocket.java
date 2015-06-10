package com.pinterest.quasar.thrift;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.fibers.io.FiberSocketChannel;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * A Thrift socket transport that expects framed messages and uses Fiber-blocking I/O.
 *
 * TFramedFiberSocket is a drop-in replacement for a TSocket (or TFiberSocket) that is wrapped with
 * a TFramedTransport or TFastFramedTransport. TFramedFiberSocket is significantly faster and more
 * scalable than using a TFiberSocket wrapped with TFastFramedTransport, as it does not need to
 * double buffer the data, and it sends the minimal number of packets over the network.
 */
public class TFramedFiberSocket extends TTransport {
  private static final Logger LOG = LoggerFactory.getLogger(TFramedFiberSocket.class);

  private static final int INITIAL_CAPACITY = 128;
  private static final float GROWTH_FACTOR = 1.5f;

  private static final int STATE_INIT = 0;
  private static final int STATE_READ = 1;
  private static final int STATE_WRITE = 2;

  private final FiberSocketChannel socketChannel;
  private final long timeout;
  private final TimeUnit timeoutUnit;
  private final ByteBuffer header;
  private ByteBuffer buffer;
  private int state;

  TFramedFiberSocket(FiberSocketChannel socketChannel, long timeout, TimeUnit timeoutUnit) {
    this.socketChannel = socketChannel;
    this.timeout = timeout;
    this.timeoutUnit = timeoutUnit;
    header = ByteBuffer.allocate(4);
    buffer = ByteBuffer.allocate(INITIAL_CAPACITY);
    state = STATE_INIT;
  }

  public static TFramedFiberSocket open(SocketAddress addr) throws IOException, SuspendExecution {
    return new TFramedFiberSocket(FiberSocketChannel.open(addr), -1, TimeUnit.SECONDS);
  }

  public static TFramedFiberSocket open(SocketAddress addr, long timeout, TimeUnit timeoutUnit)
      throws IOException, SuspendExecution {
    return new TFramedFiberSocket(FiberSocketChannel.open(addr), timeout, timeoutUnit);
  }

  @Override
  public boolean isOpen() {
    return socketChannel.isOpen();
  }

  @Override
  public void open() throws TTransportException {}

  @Override
  @Suspendable
  public void close() {
    try {
      socketChannel.close();
    } catch (IOException ioex) {
      LOG.warn("Failed to close socket channel", ioex);
    }
  }

  @Suspendable
  private void readFrame() throws TTransportException {
    try {
      header.clear();
      while (header.position() < header.capacity()) {
        LOG.info("reading header bytes");
        long bytesRead = socketChannel.read(header);
        if (bytesRead == -1) {
          throw new TTransportException(TTransportException.END_OF_FILE);
        }
      }

      header.flip();
      int len = header.getInt();

      if (buffer.capacity() < len) {
        LOG.info("allocating a new buffer of size {}", len);
        buffer = ByteBuffer.allocate(len);
      } else {
        buffer.clear();
      }

      while (buffer.position() < len) {
        LOG.info("reading body bytes with timeout");
        long bytesRead = socketChannel.read(buffer, timeout, timeoutUnit);
        if (bytesRead == -1) {
          throw new TTransportException(TTransportException.END_OF_FILE);
        }
      }

      buffer.flip();
    } catch (IOException ioex) {
      throw new TTransportException(TTransportException.UNKNOWN, ioex);
    } catch (SuspendExecution se) {
      throw new TTransportException(TTransportException.UNKNOWN, se);
    }
  }

  @Override
  @Suspendable
  public int read(byte[] bytes, int offset, int limit) throws TTransportException {
    if (state == STATE_INIT || state == STATE_WRITE) {
      readFrame();
      state = STATE_READ;
    }

    LOG.info("Returning bytes from buffer");
    buffer.get(bytes, offset, limit);
    return limit;
  }

  @Override
  @Suspendable
  public void write(byte[] bytes, int offset, int limit) throws TTransportException {
    if (state == STATE_INIT || state == STATE_READ) {
      buffer.clear();
      state = STATE_WRITE;
    }

    if (buffer.limit() - buffer.position() < limit) {
      int size = Math.max(buffer.position() + limit, (int) (buffer.capacity() * GROWTH_FACTOR));
      ByteBuffer newBuffer = ByteBuffer.allocate(size);
      buffer.rewind();
      newBuffer.put(buffer);
      buffer = newBuffer;
    }

    LOG.info("writing bytes into buffer");
    buffer.put(bytes, offset, limit);
  }

  @Override
  @Suspendable
  public void flush() throws TTransportException {
    try {
      buffer.flip();
      header.clear();
      header.putInt(buffer.remaining()).flip();
      ByteBuffer[] buffers = {header, buffer};
      while (buffer.hasRemaining()) {
        LOG.info("writing out header and body bytes");
        long bytesWritten = socketChannel.write(buffers);
        if (bytesWritten == 0) {
          throw new TTransportException(TTransportException.END_OF_FILE);
        }
      }
    } catch (IOException ioex) {
      throw new TTransportException(TTransportException.UNKNOWN, ioex);
    }
  }
}

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

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * A Thrift server that uses lightweight threads (Fibers) to process Thrift requests.
 *
 * <h2>Overview</h2>
 *
 * TFiberServer is a {@link TServer} that makes use of Quasar's {@link Fiber}s. Fibers are
 * lightweight threads for the JVM, much like Go's goroutines or Erlang's actors. A TFiberServer
 * uses N+1 Fibers to handle N connections. A single Fiber accepts incoming connections and starts
 * a new Fiber for each new connection. These Fibers read one Thrift request at a time from their
 * connection, process it, write the response and repeat until the remote client closes or there
 * is an error.
 *
 * TODO(charles): Link to a tutorial? Link to the Quasar documentation?
 *
 * <h2>Example</h2>
 *
 * Here is an example of a main method for a TFiberServer:
 *
 * <pre>
 *   public static void main(String[] args) {
 *     int port = 9999;
 *     YourService.Processor processor = new YourService.Processor();
 *     TFiberServerSocket serverSocket = new TFiberServerSocket(new InetSocketAddress(port));
 *     TFiberServer server = new TFiberServer(new TFiberServer.Args(serverSocket, processor);
 *     server.serve();
 *
 *     Runtime.addShutdownHook(new Thread() {
 *       @Override
 *       public void run() {
 *         server.stop();
 *         server.join();
 *       }
 *     });
 *
 *     server.join();
 *   }
 * </pre>
 *
 * The call to {@link java.lang.Runtime#addShutdownHook(Thread)} is optional, but makes it possible
 * to gracefully shutdown the server by stopping the JVM (typically by sending a SIGINT to the JVM).
 * The final call to {@link TFiberServer#join()} is not optional, as the main method is the only
 * non-daemon thread running in this example (so the JVM will exit if you don't call join here).
 *
 * <h2>Thread Safety Notes</h2>
 *
 * The {@link TServer#getEventHandler()} and {@link TServer#setServerEventHandler} methods are not
 * thread-safe so they should only be called immediately after the server is created, and before
 * the {@link TServer#serve()} method is called or there can be visibility issues. Event handlers
 * can be called from multiple Fibers, and must be concurrency-safe.
 */
public class TFiberServer extends TServer {
  /**
   * Arguments for a TFiberServer.
   */
  public static class Args extends AbstractServerArgs<Args> {
    /**
     * Constructor
     *
     * @param transport must be compatible with {@link TFiberServerSocket}.
     * @param processor the processor to use for all requests, must be concurrency safe.
     */
    public Args(TServerTransport transport, TProcessor processor) {
      super(transport);
      processor(processor);
    }

    /**
     * Constructor
     *
     * @param transport must be compatible with {@link TFiberServerSocket}.
     * @param processorFactory a factory to produce a processor for each connection, these do not
     *                         need to be concurrency safe (confined to one Fiber).
     */
    public Args(TServerTransport transport, TProcessorFactory processorFactory) {
      super(transport);
      processorFactory(processorFactory);
    }
  }

  /**
   * Constructor.
   *
   * @param args
   */
  public TFiberServer(AbstractServerArgs<Args> args) {
    super(args);
    serverFiber = new AcceptFiber();
    stopped = false;
  }

  /**
   * Starts a server Fiber that accepts connections and creates a Fiber to process requests for
   * each connection.
   *
   * This method can only be called once and must be called before any calls to stop or join.
   */
  @Override
  @Suspendable
  public void serve() {
    serverFiber.start();
  }

  /**
   * Signals the server Fiber to stop accepting connections, and the connection fibers to stop
   * processing new requests. Any existing requests will attempt to complete.
   *
   * This method should only be called after {@link TFiberServer#serve()}.
   */
  @Override
  @Suspendable
  public void stop() {
    stopped = true;
    serverFiber.interrupt();
  }

  @Override
  @Suspendable
  public boolean isServing() {
    return serverFiber.isAlive();
  }

  /**
   * Waits for the server Fiber to stop accepting connections and exit.
   *
   * This method should only be called after {@link TFiberServer#serve()}.
   *
   * TODO(charles): when join returns there may still be connection Fibers processing requests. That
   * makes this fairly useless for gracefully shutting down the server. How to fix this?
   */
  @Suspendable
  public void join() {
    try {
      serverFiber.join();
    } catch (InterruptedException iex) {
      LOG.debug("Interrupted while joining server Fiber", iex);
    } catch (ExecutionException eex) {
      LOG.debug("Execution failure while joining server Fiber", eex);
    }
  }

  private final class AcceptFiber extends Fiber<Void> {
    @Override
    protected Void run() throws SuspendExecution, InterruptedException {
      try {
        serverTransport_.listen();
      } catch (TTransportException ttex) {
        LOG.error("Failed to listen on server transport, exiting", ttex);
        return null;
      }

      if (eventHandler_ != null) {
        eventHandler_.preServe();
      }

      while (!Strand.interrupted()) {
        final TTransport connTransport;
        try {
          connTransport = serverTransport_.accept();
        } catch (TTransportException ttex) {
          LOG.error("Failed to accept connection from server transport, exiting", ttex);
          break;
        }

        new ConnectionFiber(connTransport).start();
      }

      stopped = true;
      serverTransport_.close();
      return null;
    }
  }

  private final class ConnectionFiber extends Fiber<Void> {
    public ConnectionFiber(TTransport t) {
      connTransport = t;
    }

    @Override
    protected Void run() throws SuspendExecution, InterruptedException {
      TProcessor processor;
      TTransport inputTrans = null;
      TTransport outputTrans = null;
      TProtocol inputProto = null;
      TProtocol outputProto = null;

      TServerEventHandler eventHandler = null;
      ServerContext connContext = null;

      try {
        processor = processorFactory_.getProcessor(connTransport);
        inputTrans = inputTransportFactory_.getTransport(connTransport);
        outputTrans = outputTransportFactory_.getTransport(connTransport);
        inputProto = inputProtocolFactory_.getProtocol(inputTrans);
        outputProto = outputProtocolFactory_.getProtocol(outputTrans);

        eventHandler = getEventHandler();
        if (eventHandler != null) {
          connContext = eventHandler.createContext(inputProto, outputProto);
        }

        while (true) {
          if (eventHandler != null) {
            eventHandler.processContext(connContext, inputTrans, outputTrans);
          }

          if (stopped || !processor.process(inputProto, outputProto)) {
            break;
          }
        }
      } catch (TTransportException ttex) {
        if (ttex.getType() != TTransportException.END_OF_FILE) {
          LOG.error("Exception while processing connection, shutting down Fiber", ttex);
        }
      } catch (Exception ex) {
        LOG.error("Exception while processing connection, shutting down Fiber", ex);
      } finally {
        closeTransports(inputTrans, outputTrans);
      }

      if (eventHandler != null) {
        eventHandler.deleteContext(connContext, inputProto, outputProto);
      }

      return null;
    }

    @Suspendable
    private void closeTransports(TTransport inputTrans, TTransport outputTrans) {
      if (inputTrans != null) {
        try {
          inputTrans.close();
        } catch (Exception ex) {
          LOG.warn("Failed to close input transport", ex);
        }
      }

      if (outputTrans != null) {
        try {
          outputTrans.close();
        } catch (Exception ex) {
          LOG.warn("Failed to close output transport", ex);
        }
      }
    }

    private final TTransport connTransport;
  }

  private final Fiber<Void> serverFiber;
  private volatile boolean stopped;

  private static final Logger LOG = LoggerFactory.getLogger(TFiberServer.class);
}

//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Bobby Johnson
// @author James Wang
// @author Jason Sobel
// @author Avinash Lakshman
// @author Anthony Giardullo

#include "common.h"
#include "scribe_server.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;

using namespace scribe::thrift;
using namespace scribe::concurrency;

using boost::shared_ptr;

void serve(shared_ptr<TNonblockingServer> server, const std::string &ip, unsigned long int port);
void listenSocket(shared_ptr<TNonblockingServer> server, const char *ip_, unsigned long int port_);

/*
 * Network configuration and directory services
 */

bool scribe::network_config::getService(const std::string& serviceName,
                                        const std::string& options,
                                        server_vector_t& _return) {
  return false;
}

/*
 * Concurrency mechanisms
 */

shared_ptr<ReadWriteMutex> scribe::concurrency::createReadWriteMutex() {
  return shared_ptr<ReadWriteMutex>(new ReadWriteMutex());
}

/*
 * Time functions
 */

unsigned long scribe::clock::nowInMsec() {
  // There is a minor race condition between the 2 calls below,
  // but the chance is really small.

  // Get current time in timeval
  struct timeval tv;
  gettimeofday(&tv, NULL);

  // Get current time in sec
  time_t sec = time(NULL);

  return ((unsigned long)sec) * 1000 + (tv.tv_usec / 1000);
}

/*
 * Hash functions
 */

uint32_t scribe::integerhash::hash32(uint32_t key) {
  return key;
}

uint32_t scribe::strhash::hash32(const char *s) {
  // Use the djb2 hash (http://www.cse.yorku.ca/~oz/hash.html)
  if (s == NULL) {
    return 0;
  }
  uint32_t hash = 5381;
  int c;
  while ((c = *s++)) {
    hash = ((hash << 5) + hash) + c; // hash * 33 + c
  }
  return hash;
}

/*
 * Starting a scribe server.
 */
// note: this function uses global g_Handler.
void scribe::startServer() {
  boost::shared_ptr<TProcessor> processor(new scribeProcessor(g_Handler));
  /* This factory is for binary compatibility. */
  boost::shared_ptr<TProtocolFactory> protocol_factory(
    new TBinaryProtocolFactory(0, 0, false, false)
  );
  boost::shared_ptr<ThreadManager> thread_manager;

  if (g_Handler->numThriftServerThreads > 1) {
    // create a ThreadManager to process incoming calls
    thread_manager = ThreadManager::newSimpleThreadManager(
      g_Handler->numThriftServerThreads
    );

    shared_ptr<PosixThreadFactory> thread_factory(new PosixThreadFactory());
    thread_manager->threadFactory(thread_factory);
    thread_manager->start();
  }

  shared_ptr<TNonblockingServer> server(new TNonblockingServer(
                                          processor,
                                          protocol_factory,
                                          g_Handler->port,
                                          thread_manager
                                        ));
  g_Handler->setServer(server);

  LOG_OPER("Starting scribe server on port %lu", g_Handler->port);
  if (!g_Handler->ip.empty()) LOG_OPER("Binding to %s", g_Handler->ip.c_str());
  fflush(stderr);

  // throttle concurrent connections
  unsigned long mconn = g_Handler->getMaxConn();
  if (mconn > 0) {
    LOG_OPER("Throttle max_conn to %lu", mconn);
#ifdef T_OVERLOAD_CLOSE_ON_ACCEPT
    server->setMaxConnections(mconn);
    server->setOverloadAction(T_OVERLOAD_CLOSE_ON_ACCEPT);
#endif
  }

  serve(server, g_Handler->ip, g_Handler->port);
  // this function never returns
}


/*
 * Stopping a scribe server.
 */
void scribe::stopServer() {
  exit(0);
}

/**
 * Main workhorse function, starts up the server listening on a port and
 * loops over the libevent handler.
 */
void serve(shared_ptr<TNonblockingServer> server, const std::string &ip, unsigned long int port) {
  // Init socket
  listenSocket(server, ip.empty() ? NULL : ip.c_str(), port);

  if (server->isThreadPoolProcessing()) {
    // Init task completion notification pipe
    server->createNotificationPipe();
  }

  // Initialize libevent core
  server->registerEvents(static_cast<event_base*>(event_init()));

  // Run the preServe event
  if (server->getEventHandler() != NULL) {
    server->getEventHandler()->preServe();
  }

  // Run libevent engine, never returns, invokes calls to eventHandler
  event_base_loop(server->getEventBase(), 0);
}

void listenSocket(shared_ptr<TNonblockingServer> server, const char *ip_, unsigned long int port_) {
  int s;
  struct addrinfo hints, *res, *res0;
  int error;

  char port[sizeof("65536") + 1];
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = PF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE | AI_ADDRCONFIG;
  sprintf(port, "%d", port_);

  // Wildcard address
  error = getaddrinfo(ip_, port, &hints, &res0);
  if (error) {
    std::string errStr = "TNonblockingServer::serve() getaddrinfo " + std::string(gai_strerror(error));
    GlobalOutput(errStr.c_str());
    return;
  }

  // Pick the ipv6 address first since ipv4 addresses can be mapped
  // into ipv6 space.
  for (res = res0; res; res = res->ai_next) {
    if (res->ai_family == AF_INET6 || res->ai_next == NULL)
      break;
  }

  // Create the server socket
  s = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (s == -1) {
    freeaddrinfo(res0);
    throw TException("TNonblockingServer::serve() socket() -1");
  }

  #ifdef IPV6_V6ONLY
  if (res->ai_family == AF_INET6) {
    int zero = 0;
    if (-1 == setsockopt(s, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero))) {
      GlobalOutput("TServerSocket::listen() IPV6_V6ONLY");
    }
  }
  #endif // #ifdef IPV6_V6ONLY


  int one = 1;

  // Set reuseaddr to avoid 2MSL delay on server restart
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  if (bind(s, res->ai_addr, res->ai_addrlen) == -1) {
    close(s);
    freeaddrinfo(res0);
    throw TException("TNonblockingServer::serve() bind");
  }

  // Done with the addr info
  freeaddrinfo(res0);

  // Set up this file descriptor for listening
  server->listenSocket(s);
}

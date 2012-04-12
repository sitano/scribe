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
// See accompanying file LICENSE

#ifndef MYSQLSTORE_H_
#define MYSQLSTORE_H_

#ifdef USE_SCRIBE_MYSQL

#include "common.h"
#include "store.h"
#include <mysql/mysql.h>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#define DEFAULT_MYSQL_PORT 3310

/*
 * This store sends messages to a Cassandra Server.
 */
class MysqlStore: public Store {
public:
    MysqlStore(StoreQueue* storeq, const std::string& category,
            bool multi_category);
    ~MysqlStore();

    boost::shared_ptr<Store> copy(const std::string &category);
    bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
    bool open();
    bool isOpen();
    void configure(pStoreConf configuration, pStoreConf parent);
    void close();
    void flush();
    void periodicCheck();

protected:
    static const long int DEFAULT_SOCKET_TIMEOUT_MS = 5000; // 5 sec timeout

    // configuration
    long int timeout;
    long int remotePort;
    std::string remoteHost;
    std::tr1::shared_ptr<libcassandra::Cassandra> client;

    // state
    bool opened;

private:
    // disallow copy, assignment, and empty construction
    MysqlStore();
    MysqlStore(MysqlStore& rhs);
    MysqlStore& operator=(MysqlStore& rhs);
};

#endif

#endif


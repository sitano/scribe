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

#ifndef CASSANDRA_STORAGE_H
#define CASSANDRA_STORAGE_H

#ifdef USE_SCRIBE_CASSANDRA

#include "common.h"
#include "store.h"
#include <jansson.h>
#include "libcassandra/cassandra_factory.h"
#include "libcassandra/cassandra.h"
#include "libcassandra/keyspace.h"
#include <boost/foreach.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#define DEFAULT_CASSANDRA_PORT 9160

/*
 * This store sends messages to a Cassandra Server.
 */
class CassandraStore: public Store {
public:
    CassandraStore(StoreQueue* storeq, const std::string& category,
            bool multi_category);
    ~CassandraStore();

    boost::shared_ptr<Store> copy(const std::string &category);
    bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
    bool open();
    bool isOpen();
    void configure(pStoreConf configuration, pStoreConf parent);
    void close();
    void flush();
    void periodicCheck();

protected:
    struct CassandraDataStruct {
        std::string columnFamily;
        std::string superColumnFamily;
        std::string rowKey;
        std::string columnName;
        std::string value;
        bool counter;
    };

    static const long int DEFAULT_SOCKET_TIMEOUT_MS = 5000; // 5 sec timeout

    // configuration
    long int timeout;
    long int remotePort;
    std::string remoteHost;
    bool categoryAsCfName;
    std::string keyspace;
    std::string columnFamily;
    org::apache::cassandra::ConsistencyLevel::type consistencyLevel;
    std::tr1::shared_ptr<libcassandra::Cassandra> client;

    // state
    bool opened;

private:
    bool createInsertTuple(std::string message,
            std::vector<libcassandra::Cassandra::SuperColumnInsertTuple>* scit,
            std::vector<libcassandra::Cassandra::ColumnInsertTuple>* cit);
    bool getColumnStringValue(json_t* root, std::string key, std::string& _return);
    std::vector<CassandraStore::CassandraDataStruct>* parseJsonMessage(std::string message);

    // disallow copy, assignment, and empty construction
    CassandraStore();
    CassandraStore(CassandraStore& rhs);
    CassandraStore& operator=(CassandraStore& rhs);
};

#endif

#endif

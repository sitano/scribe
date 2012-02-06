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

#include "CassandraStore.h"

using namespace std;
using namespace boost;
using namespace boost::iostreams;
using namespace libcassandra;

#ifdef USE_SCRIBE_CASSANDRA

CassandraStore::CassandraStore(StoreQueue* storeq, const string& category,
        bool multi_category) :
    Store(storeq, category, "cassandra", multi_category), gzip(false),
            categoryAsCfName(false), keyspace(""), columnFamily(""),
            opened(false) {
    // we can't open the connection until we get configured
}

CassandraStore::~CassandraStore() {
    close();
}

void CassandraStore::configure(pStoreConf configuration, pStoreConf parent) {
    Store::configure(configuration, parent);
    // Error checking is done on open()
    if (!configuration->getString("remote_host", remoteHost))  {
        throw runtime_error("Bad Config - remote_host not set");
        LOG_OPER("[%s] Bad Config - remote_host not set", categoryHandled.c_str());
    }

    if (!configuration->getInt("remote_port", remotePort)) {
        remotePort = DEFAULT_CASSANDRA_PORT;
    }

    if (!configuration->getInt("timeout", timeout)) {
        timeout = DEFAULT_SOCKET_TIMEOUT_MS;
    }

    configuration->getBool("category_as_cf_name", categoryAsCfName);

    if (!configuration->getString("keyspace", keyspace)) {
        LOG_OPER("[%s] Bad Config - Keyspace not set", categoryHandled.c_str());
    }

    if (configuration->getString("column_family", columnFamily)
            && categoryAsCfName) {
        LOG_OPER("[%s] Bad Config - category_is_cf_name = 'yes' and column_family set", categoryHandled.c_str());
    }

    configuration->getBool("gzip", gzip);
}

void CassandraStore::periodicCheck() {
    // nothing for now
}

bool CassandraStore::open() {
    if (isOpen()) {
        return (true);
    }
    opened = true;
    try {
        CassandraFactory factory(remoteHost, remotePort);
        tr1::shared_ptr<Cassandra> client_(factory.create());
        client = client_;
    }
    catch (apache::thrift::TException& e) {
        cout << e.what() << endl;
        opened = false;
    }
    catch (std::exception e) {
        cout << e.what() << endl;
        opened = false;
    }

    if (opened) {
        // clear status on success
        setStatus("");
    } else {
        setStatus("Failed to connect");
    }
    return opened;
}

void CassandraStore::close() {
    if (opened) {
        LOG_OPER("[%s] [cassandra] disconnected client", categoryHandled.c_str());
    }
    opened = false;
}

bool CassandraStore::isOpen() {
    return opened;
}

shared_ptr<Store> CassandraStore::copy(const std::string &category) {
    CassandraStore *store = new CassandraStore(storeQueue, category,
            multiCategory);
    shared_ptr<Store> copied = shared_ptr<Store> (store);
    store->timeout = timeout;
    store->remoteHost = remoteHost;
    store->remotePort = remotePort;
    store->gzip = gzip;
    store->categoryAsCfName = categoryAsCfName;
    store->keyspace = keyspace;
    store->columnFamily = columnFamily;

    return copied;
}

bool CassandraStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
    bool success = true;
    if (!isOpen()) {
        success = open();
        if (!success) {
            return false;
        }
    }

    vector<Cassandra::SuperColumnInsertTuple> *scit = new std::vector<
            Cassandra::SuperColumnInsertTuple>();
    vector<Cassandra::ColumnInsertTuple> *cit = new std::vector<
            Cassandra::ColumnInsertTuple>();

    for (logentry_vector_t::iterator iter = messages->begin();
           iter != messages->end();
           ++iter) {

        string message;
        stringstream gzMessage;
        stringstream rawMessage;
//        cout << "size: " << sizeof(message) << "length: " << message.length() << endl;
//        printf("%x - %x - %x - %x", (*iter)->message.at(0),
//                (*iter)->message.at(1),
//                (unsigned int)(*iter)->message.at(2),
//                (unsigned int)(*iter)->message.at(3));
        if ((unsigned int) (*iter)->message[0] == 0x1f && (unsigned int) (*iter)->message[1] == 0xffffff8b) {
            cout << message << endl;
            gzMessage << (*iter)->message;
            boost::iostreams::filtering_streambuf<boost::iostreams::input> gzFilter;
            gzFilter.push(gzip_decompressor());
            gzFilter.push(gzMessage);
            boost::iostreams::copy(gzFilter, rawMessage);
            message = rawMessage.str();
//            cout << "ungzipped: " << rawMessage.str() << endl;
        }
        else {
            message = (*iter)->message;
        }

        string rowKey;
        string scName;
        if (!parseJsonMessage(message, rowKey, scName, scit, cit)) {
            LOG_OPER("could not create insert touple for <%s>", message.c_str());
        }
    }

    if (scit->size() > 0 || cit->size() > 0) {
        try {
            client->setKeyspace(keyspace);
            // TODO: make ConsistencyLevel configurable
            unsigned long start = scribe::clock::nowInMsec();
            client->batchInsert(*cit, *scit, org::apache::cassandra::ConsistencyLevel::ONE);
            unsigned long runtime = scribe::clock::nowInMsec() - start;
            LOG_OPER("[%s] [Cassandra] [%s] wrote %lu super columns and %lu columns in <%lu>",
                    categoryHandled.c_str(), client->getHost().c_str(), scit->size(),
                    cit->size(), runtime);
        } catch (org::apache::cassandra::InvalidRequestException &ire) {
            cout << ire.why << endl;
            success = false;
        }
        catch (std::exception& e) {
            close();
            cout << e.what() << endl;
            success = false;
        }
    } else {
        LOG_OPER("[%s] [Cassandra] nothing to write", categoryHandled.c_str());
    }

    return success;
}

bool CassandraStore::getColumnStringValue(json_t* root, string key, string& _return) {
    json_t* jObj = (key.empty()) ? root : json_object_get(root, key.c_str());
    if (jObj) {
        int type = json_typeof(jObj);
        stringstream stream;
        switch (type) {
            case JSON_STRING:
                _return = json_string_value(jObj);
                return true;
            case JSON_INTEGER:
                stream << (uint64_t)json_integer_value(jObj);
                _return = stream.str();
                return true;
            default:
                LOG_OPER("[%s] [cassandra][ERROR] value format not valid", categoryHandled.c_str());
                return false;
        }
        return false;
    }
    return false;
}

bool CassandraStore::parseJsonMessage(string message, string& rowKey,
        string& scName, vector<Cassandra::SuperColumnInsertTuple>* scit,
        vector<Cassandra::ColumnInsertTuple>* cit) {
    if (message.empty()) {
        LOG_DBG("empty Message");
        return true;
    }

    json_error_t error;
    json_t* jsonRoot = json_loads(message.c_str(), 0, &error);
    if (jsonRoot) {
        LOG_DBG("json parsed");
        // get rowKey which is required
        if (!getColumnStringValue(jsonRoot, "rowKey", rowKey)) {
            LOG_OPER("[cassandra][ERROR] rowKey not set %s", message.c_str());
            return false;
        }
        LOG_DBG("rowKey: %s", rowKey.c_str());

        // get optional super column name
        string scName;
        getColumnStringValue(jsonRoot, "scName", scName);
        LOG_DBG("scName: %s", scName.c_str());

        // get actual column data
        json_t *dataObj = json_object_get(jsonRoot, "data");
        if (json_is_object(dataObj)) {
            const char* key;
            json_t* jValueObj;
            json_object_foreach(dataObj, key, jValueObj) {
                string columnValue;
                if (!getColumnStringValue(jValueObj, "", columnValue)) {
                    LOG_DBG("could not get value for %s", key);
                }

                string columnFamily_ = (categoryAsCfName) ? categoryHandled.c_str() : columnFamily;
                if (scName.empty()) {
                    Cassandra::ColumnInsertTuple t(columnFamily_,
                            rowKey.c_str(), key, columnValue);
                    cit->push_back(t);
                }
                else {
                    Cassandra::SuperColumnInsertTuple t(columnFamily_,
                            rowKey.c_str(), scName.c_str(), key, columnValue);
                    scit->push_back(t);
                }

                LOG_DBG("type %i", json_typeof(jValueObj));
                LOG_DBG("key %s", key);
                LOG_DBG("value %s", columnValue.c_str());

            }
        } else {
            LOG_OPER("[cassandra][ERROR] data not set - at least one value is required: %s", message.c_str());
            return false;
        }

        json_decref(jsonRoot);
    } else {
        LOG_OPER("[cassandra][ERROR] Not a valid JSON String '%s'", message.c_str());
        return false;
    }
    return true;
}

void CassandraStore::flush() {
    // Nothing to do
}

#endif

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

/**
 * SQL INSERT/UPDATE Statement gzip compressed!
 */

#ifdef USE_SCRIBE_MYSQL

#include "MysqlStore.h"

using namespace std;

void MysqlStore::configure(pStoreConf configuration, pStoreConf parent) {
    Store::configure(configuration, parent);
    // Error checking is done on open()
    if (!configuration->getString("remote_host", remoteHost)) {
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
}

#endif

#include <limits>
#include "common.h"

#ifdef USE_SCRIBE_CASSANDRA

#include "file.h"
#include "conn_pool.h"
#include <uuid/uuid.h>
#include "CassandraStorage.h"

using std::string;
using boost::shared_ptr;
using namespace libcassandra;
using namespace std;

bool CassandraStorage::connectToPath(const char *uri) {
    connected = false;    
    const char proto[] = "cassandra://";
    
    if (strncmp(proto, uri, strlen(proto)) != 0) {
        LOG_OPER("[cassandra] Invalid URI for a cassandra resource: %s", uri);      
    }
    uri += strlen(proto);
    const char * colon = strchr(uri, ':');
    if (!colon || !colon[1]) {
        LOG_OPER("[cassandra] Missing port specification: \"%s\"", uri);
        return false;
    }
    
    char * endpnt = NULL;
    long port = strtol(colon + 1, &endpnt, 10);
    if (endpnt == colon + 1) {
        port = DEFAULT_CASSANDRA_PORT;
    }
    const char *pnt = endpnt;
    while (*pnt == '/') {
        pnt++;
    }
    if (port < 0) {
        LOG_OPER("[cassandra] Invalid port specification (negative): \"%s\"", uri);
        return false;
    } else if (port > 65535) {
        LOG_OPER("[casssandra] Invalid port specification (out of range): \"%s\"", uri);
        return false;
    }
    string host(uri, colon - uri);    
    if (*pnt == 0) {
        LOG_OPER("[casssandra] Missing keyspace name: \"%s\"", uri);
        return false;
    }
    while (*pnt == '/') {
        pnt++;
    }
    colon = strchr(pnt, '/');
    if (!*pnt || colon == pnt || !colon || !colon[1]) {
        LOG_OPER("[casssandra] Missing keyspace name: \"%s\"", uri);        
        return false;
    }
    string kspName_(pnt, colon - pnt);
    
    pnt = colon + 1;
    while (*pnt == '/') {
        pnt++;
    }
    colon = strchr(pnt, '/');
    if (!colon || colon == pnt || !colon[1]) {
        LOG_OPER("[casssandra] Missing column family name: \"%s\"", uri);
        return false;
    }
    string cfName_(pnt, colon - pnt);    
    
    pnt = colon + 1;
    while (*pnt == '/') {
        pnt++;
    }
    string fileName_(pnt);
    
    kspName = new string(kspName_);
    cfName = new string(cfName_);
    fileName = new string(fileName_);
    CassandraFactory factory(host, port);
    tr1::shared_ptr<Cassandra> client_(factory.create());
    client = client_;
    connected = true;
    LOG_OPER("Opened connection to remote Cassandra server [%s:%ld] [%s] [%s] [%s]", host.c_str(), port, kspName->c_str(), cfName->c_str(), fileName->c_str());
    
    return true;
}

CassandraStorage::CassandraStorage(const std::string& name) : FileInterface(name, false), inputBuffer_(NULL), bufferSize_(0), connected(false), kspName(NULL), cfName(NULL), fileName(NULL) {
    LOG_OPER("[cassandra] Connecting to cassandra for %s", name.c_str());
    connectToPath(name.c_str());
    if (!connected) {
        LOG_OPER("[cassandra] ERROR: Cassandra is not configured for uri: %s", name.c_str());
    }
}

CassandraStorage::~CassandraStorage() {
    if (connected) {
        LOG_OPER("[cassandra] disconnected Cassandra for %s", filename.c_str());        
    }
}

bool CassandraStorage::openRead() {
    if (!connected) {
        connectToPath(filename.c_str());
    }
    return connected;
}

bool CassandraStorage::openWrite() {
    if (!connected) {
        connectToPath(filename.c_str());
    }
    return connected;
}

bool CassandraStorage::openTruncate() {
    LOG_OPER("[cassandra] truncate %s", filename.c_str());
    deleteFile();
    return openWrite();
}

bool CassandraStorage::isOpen() {
    return connected;
}

void CassandraStorage::close() {
    if (connected) {
        LOG_OPER("[cassandra] disconnected for %s", filename.c_str());
    }
    connected = false;
}

bool CassandraStorage::writeEntry(const std::string& data) {
    if (data.length() <= 0U) {
        return true;
    }
    try {
        uuid_t uuid_c;
        uuid_generate_time(uuid_c);
        std::string uuid((char *) uuid_c, 16);
        Keyspace *ksp = client->getKeyspace(*kspName);
        ksp->insertColumn(*fileName, *cfName, uuid, data);
    } catch (org::apache::cassandra::InvalidRequestException &ire) {
        cout << ire.why << endl;
        return false;
    }    
    return true;
}

bool CassandraStorage::write(const std::string& data) {
    if (!isOpen()) {
        bool success = openWrite();
        
        if (!success) {
            return false;
        }
    }
    bool ret = true;    
    size_t start = 0U, found;    
    do {
        found = data.find_first_of('\n', start + 1U);;
        if (found == string::npos) {
            ret &= writeEntry(data.substr(start));
            break;
        }
        ret &= writeEntry(data.substr(start, found - start));        
        start = found + 1U;
    } while (start < data.length());
    
    return ret;
}

void CassandraStorage::flush() {

}

unsigned long CassandraStorage::fileSize() {
    long size = 0L;
    
    if (connected) {
        org::apache::cassandra::ColumnParent col_parent;
        col_parent.column_family = *cfName;
        Keyspace *ksp = client->getKeyspace(*kspName);
        size = ksp->getCount(*fileName, col_parent);
    }    
    return size;
}

void CassandraStorage::deleteFile() {
    if (connected) {
        Keyspace *ksp = client->getKeyspace(*kspName);
        ksp->remove(*fileName, *cfName, NULL, NULL);
    }
    LOG_OPER("[cassandra] deleteFile %s", filename.c_str());
}

void CassandraStorage::listImpl(const std::string& path,
                                std::vector<std::string>& _return) {

}

bool CassandraStorage::readNext(std::string& _return) {
   return false;           // frames not yet supported
}

string CassandraStorage::getFrame(unsigned data_length) {
  return std::string();    // not supported
}

bool CassandraStorage::createDirectory(std::string path) {
  // opening the file will create the directories.
  return true;
}

/**
 * cassandra currently does not support symlinks. So we create a
 * normal file and write the symlink data into it
 */
bool CassandraStorage::createSymlink(std::string oldpath, std::string newpath) {
    bool ret = writeEntry(oldpath);
    
    return ret;
}

#endif

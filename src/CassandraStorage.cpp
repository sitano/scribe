#include <limits>
#include "common.h"

#ifdef USE_SCRIBE_CASSANDRA

#include "file.h"
#include "conn_pool.h"
#include <uuid/uuid.h>
#include "CassandraStorage.h"
#include <boost/tokenizer.hpp>
#include <iostream>
#include <map>

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
    colon = strchr(pnt, '/');
    if (!colon) {
        categoryName = new string(pnt);
        fileName = new string(*categoryName);
    } else {
        categoryName = new string(pnt, colon - pnt);
        pnt = colon + 1;
        while (*pnt == '/') {
            pnt++;
        }
        fileName = new string(pnt);
    }

    kspName = new string(kspName_);
    cfName = new string(cfName_);
    CassandraFactory factory(host, port);
    tr1::shared_ptr<Cassandra> client_(factory.create());
    client = client_;
    connected = true;
    LOG_OPER("Opened connection to remote Cassandra server [%s:%ld] [%s] [%s] [%s] [%s]",
             host.c_str(),
             port,
             kspName->c_str(),
             cfName->c_str(),
             categoryName->c_str(),
             fileName->c_str());

    return true;
}

CassandraStorage::CassandraStorage(const std::string& name) :
  FileInterface(name, false),
  inputBuffer_(NULL),
  bufferSize_(0),
  connected(false),
  kspName(NULL),
  cfName(NULL),
  categoryName(NULL),
  fileName(NULL) {
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

void CassandraStorage::writeEntry(std::vector<Cassandra::SuperColumnInsertTuple> *scit, const std::string& data) {
    uuid_t uuid_c;
    uuid_generate_time(uuid_c);
    std::string uuid((char *) uuid_c, 16);
    std::string cn = string("data");

    std::map<std::string, std::string> values;
    typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
    boost::char_separator<char> sep("##");
    tokenizer tokens(data, sep);
    for (tokenizer::iterator tok_iter = tokens.begin();tok_iter != tokens.end(); ++tok_iter) {
        //cout << *tok_iter << endl;
        boost::char_separator<char> keyValueSep(":");
        tokenizer keyValue(*tok_iter, keyValueSep);
        std::vector<std::string> collection;
        for (tokenizer::iterator keyValue_tok_iter = keyValue.begin(); keyValue_tok_iter != keyValue.end(); ++keyValue_tok_iter) {
            collection.push_back(*keyValue_tok_iter);
        }
        values[collection.at(0).c_str()] = collection.at(1).c_str();
    }


    if (strcmp(values["csc"].c_str(), "")== 0) {
        cout << "MISSING CSC!" << endl;
        return;
    }
    if (strcmp(values["ckey"].c_str(), "")== 0) {
        cout << "MISSING CKEY!" << endl;
        return;
    }
    cout << "added value" << endl;
    cout << values["csc"].c_str() << " -- " << values["ckey"].c_str() << endl;


    //<< " -- " << values["data"].c_str() << endl;

    // CF, KEY, SCF, COLUM NAME, VALUE
    //LOG_OPER("[debug] %s", values["csc"].c_str());
    LOG_OPER("writing");
    Cassandra::SuperColumnInsertTuple t(*categoryName, values["ckey"].c_str(), values["csc"].c_str(), cn, values["data"].c_str());
    scit->push_back(t);
}

//csc:bla##ckey:##BYTES##

bool CassandraStorage::write(const std::string& data) {
    if (!isOpen()) {
        bool success = openWrite();

        if (!success) {
            return false;
        }
    }
    bool ret = true;
    size_t start = 0U, found;

    std::vector<Cassandra::SuperColumnInsertTuple> *scit = new std::vector<Cassandra::SuperColumnInsertTuple>();
    std::vector<Cassandra::ColumnInsertTuple> cit;

    do {
        found = data.find_first_of('\n', start + 1U);;
        if (found == string::npos) {
           writeEntry(scit, data.substr(start));
           break;
        }
        writeEntry(scit, data.substr(start, found - start));
        start = found + 1U;
    } while (start < data.length());

    cout << "Hi" << (*scit).size() << endl;

    try {
        client->setKeyspace(*kspName);
        client->batchInsert(cit, *scit);
    } catch (org::apache::cassandra::InvalidRequestException &ire) {
        cout << ire.why << endl;
        return false;
    }

    return ret;
}

void CassandraStorage::flush() {

}

unsigned long CassandraStorage::fileSize() {
    long size = 0L;

    if (connected) {
        org::apache::cassandra::ColumnParent col_parent;
        col_parent.column_family = *cfName;
        col_parent.super_column = *fileName;
        client->setKeyspace(*kspName);
        size = 1024;
        //client->getCount(*categoryName, col_parent);
    }
    return size;
}

void CassandraStorage::deleteFile() {
    if (connected) {
        client->setKeyspace(*kspName);
        client->remove(*categoryName, *cfName, *fileName, NULL);
    }
    LOG_OPER("[cassandra] deleteFile %s", fileName->c_str());
}

void CassandraStorage::listImpl(const std::string& path,
                                std::vector<std::string>& _return) {

}

long CassandraStorage::readNext(std::string& _return) {
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
    return false;
}

#endif


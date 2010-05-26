#ifndef CASSANDRA_STORAGE_H
#define CASSANDRA_STORAGE_H

#ifdef USE_SCRIBE_CASSANDRA

#include "libcassandra/cassandra_factory.h"
#include "libcassandra/cassandra.h"
#include "libcassandra/keyspace.h"

#define DEFAULT_CASSANDRA_PORT 9162

class CassandraStorage : public FileInterface {
 public:
  CassandraStorage(const std::string& name);
  virtual ~CassandraStorage();

  static void init();        // initialize cassandra subsystem
  bool openRead();           // open for reading file
  bool openWrite();          // open for appending to file
  bool openTruncate();       // truncate and open for write
  bool isOpen();             // is file open?
  void close();
  bool write(const std::string& data);
  void flush();
  unsigned long fileSize();
  bool readNext(std::string& _return);
  void deleteFile();
  void listImpl(const std::string& path, std::vector<std::string>& _return);
  std::string getFrame(unsigned data_size);
  bool createDirectory(std::string path);
  bool createSymlink(std::string newpath, std::string oldpath);

 private:
  char* inputBuffer_;
  unsigned bufferSize_;
  bool connected;
  std::tr1::shared_ptr<libcassandra::Cassandra> client;
  std::string *kspName;
  std::string *cfName;    
  std::string *fileName;    
    
  // disallow copy, assignment, and empty construction
  CassandraStorage();
  CassandraStorage(CassandraStorage& rhs);
  CassandraStorage& operator=(CassandraStorage& rhs);
    
  bool connectToPath(const char *uri);
  bool writeEntry(const std::string& data);
};

/**
 * A static lock
 */
class cassandraLock {
  private:
    static bool lockInitialized;

  public:
    static pthread_mutex_t lock;
    static bool initLock() {
      pthread_mutex_init(&lock, NULL);
      return true;
    }
};

#endif

#endif

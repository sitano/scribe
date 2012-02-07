#ifndef SCRIBE_BUCKETFALLBACKSTORE_H
#define SCRIBE_BUCKETFALLBACKSTORE_H

#include "common.h"
#include "store.h"

class BucketFallbackStore: public BucketStore {
public:
    BucketFallbackStore(StoreQueue* storeq, const std::string& category,
            bool multi_category);
    ~BucketFallbackStore();
    void configure(pStoreConf configuration, pStoreConf parent);
    boost::shared_ptr<Store> copy(const std::string &category);
    bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
    //bool open();
    void periodicCheck();

protected:
    std::vector< boost::shared_ptr<Store> > deadBuckets;

private:
    // disallow copy, assignment, and emtpy construction
    BucketFallbackStore();
    BucketFallbackStore(BucketFallbackStore& rhs);
    BucketFallbackStore& operator=(BucketFallbackStore);
    /*    void createBucketsFromBucket(pStoreConf configuration,
     pStoreConf bucket_conf);
     void createBuckets(pStoreConf configuration);*/
};

#endif

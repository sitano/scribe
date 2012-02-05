/**
 * Same as the Bucket Store except it tries another bucket if the target bucket is not available.
 * This continues until all Buckets are marked as unavailable
 */

#include "common.h"
#include "BucketFallbackStore.h"

using namespace std;
using namespace boost;
using namespace scribe::thrift;

BucketFallbackStore::BucketFallbackStore(StoreQueue* storeq,
        const string& category, bool multi_category) :
    BucketStore(storeq, category, multi_category) {
    bucketType = random;
}

BucketFallbackStore::~BucketFallbackStore() {
}

/**
 * Copies the Store for each Category
 */
shared_ptr<Store> BucketFallbackStore::copy(const std::string &category) {
  BucketFallbackStore *store = new BucketFallbackStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->numBuckets = numBuckets;
  store->bucketType = bucketType;
  store->delimiter = delimiter;

  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    store->buckets.push_back((*iter)->copy(category));
  }

  return copied;
}

/*
 * Bucketize <messages> and try to send to each contained bucket store
 * At the end of the function <messages> will contain all the messages that
 * could not be processed
 * Returns true if all messages were successfully sent, false otherwise.
 */
bool BucketFallbackStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
    bool success = true;

    boost::shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
    vector<shared_ptr<logentry_vector_t> > bucketed_messages;
    bucketed_messages.resize(numBuckets + 1);

    if (numBuckets == 0) {
        LOG_OPER("[%s] Failed to write - no buckets configured",
                categoryHandled.c_str());
        setStatus("Failed write to bucket store");
        return false;
    }

    // batch messages by bucket
    for (logentry_vector_t::iterator iter = messages->begin(); iter
            != messages->end(); ++iter) {
        unsigned bucket = bucketize((*iter)->message);

        if (!bucketed_messages[bucket]) {
            bucketed_messages[bucket] = shared_ptr<logentry_vector_t> (
                    new logentry_vector_t);
        }

        bucketed_messages[bucket]->push_back(*iter);
    }

    // handle all batches of messages
    for (unsigned long i = 0; i <= numBuckets; i++) {
        shared_ptr<logentry_vector_t> batch = bucketed_messages[i];

        if (batch) {
            if (removeKey) {
                // Create new set of messages with keys removed
                shared_ptr<logentry_vector_t> key_removed = shared_ptr<
                        logentry_vector_t> (new logentry_vector_t);

                for (logentry_vector_t::iterator iter = batch->begin(); iter
                        != batch->end(); ++iter) {
                    logentry_ptr_t entry = logentry_ptr_t(new LogEntry);
                    entry->category = (*iter)->category;
                    entry->message = getMessageWithoutKey((*iter)->message);
                    key_removed->push_back(entry);
                }
                batch = key_removed;
            }

            if (!buckets[i]->handleMessages(batch)) {
                // keep track of messages that were not handled
                failed_messages->insert(failed_messages->end(),
                        bucketed_messages[i]->begin(),
                        bucketed_messages[i]->end());
                // Bucket seems to be dead - temporarely remove it
                LOG_OPER('Bucket number %lu of type %s down', i, buckets[i]->getType().c_str());

                deadBuckets.push_back(buckets[i]);
                buckets.erase(buckets.begin() + i);
                numBuckets--;
                stringstream msg;
                msg << "Buckets not available: " << deadBuckets.size();
                setStatus(msg.str());
                if (numBuckets <= 0) {
                    success = false;
                    string msg("No Buckets Available");
                    setStatus(msg);
                }
                else {
                    // retry failed messages with remaining Buckets
                    success = handleMessages(failed_messages);
                }
            }
        }
    }

    if (!success) {
        // return failed logentrys in messages
        messages->swap(*failed_messages);
    }

    return success;
}

void BucketFallbackStore::periodicCheck() {
    BucketStore::periodicCheck();
    vector<unsigned int> bucketsToRemove;

    if (!deadBuckets.empty()) {
        LOG_OPER("we have %i dead Buckets", deadBuckets.size());
        // check if dead stores can be reached
        unsigned int size = deadBuckets.size();
        for (unsigned int i = 0; i < size; ++i) {
            if (!deadBuckets[i]->open()) {
                close();
            }
            else {
                numBuckets++;
                buckets.push_back(deadBuckets[i]);
                bucketsToRemove.push_back(i);
                LOG_OPER("[%s] Bucket #%i alive", categoryHandled.c_str(), i);
            }
        }

        if (!bucketsToRemove.empty()) {
            // remove alive Buckets
            size = bucketsToRemove.size();
            for (unsigned int i = 0; i < size; ++i) {
                deadBuckets.erase(deadBuckets.begin() + bucketsToRemove[i]);
            }
        }
    }

    // set Status
    if (deadBuckets.empty()) {
        setStatus("");
    }
    else {
        if (numBuckets > 0) {
            stringstream msg;
            msg << "Buckets not available: " << deadBuckets.size();
            setStatus(msg.str());
        }
    }

}

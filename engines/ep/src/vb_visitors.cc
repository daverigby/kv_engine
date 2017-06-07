#include "vb_visitors.h"

PauseResumeVBAdapter::PauseResumeVBAdapter(
        std::unique_ptr<HashTableVisitor> htVisitor)
    : htVisitor(std::move(htVisitor)) {
}

bool PauseResumeVBAdapter::visit(uint16_t vbucket_id, HashTable& ht) {
    // Check if this vbucket_id matches the position we should resume
    // from. If so then call the visitor using our stored HashTable::Position.
    HashTable::Position ht_start;
    if (resume_vbucket_id == vbucket_id) {
        ht_start = hashtable_position;
    }

    hashtable_position = ht.pauseResumeVisit(*htVisitor, ht_start);

    if (hashtable_position != ht.endPosition()) {
        // We didn't get to the end of this hashtable. Record the vbucket_id
        // we got to and return false.
        resume_vbucket_id = vbucket_id;
        return false;
    } else {
        return true;
    }
}

#include "vb_visitors.h"

#include "vbucket.h"

PauseResumeVBAdapter::PauseResumeVBAdapter(
        std::unique_ptr<VBucketAwareHTVisitor> htVisitor)
    : htVisitor(std::move(htVisitor)) {
}

bool PauseResumeVBAdapter::visit(VBucket& vb) {
    // Check if this vbucket_id matches the position we should resume
    // from. If so then call the visitor using our stored HashTable::Position.
    HashTable::Position ht_start;
    if (resume_vbucket_id == vb.getId()) {
        ht_start = hashtable_position;
    }

    htVisitor->setCurrentVBucket(vb);
    hashtable_position = vb.ht.pauseResumeVisit(*htVisitor, ht_start);

    if (hashtable_position != vb.ht.endPosition()) {
        // We didn't get to the end of this VBucket. Record the vbucket_id
        // we got to and return false.
        resume_vbucket_id = vb.getId();
        return false;
    } else {
        return true;
    }
}

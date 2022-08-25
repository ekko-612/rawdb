#include <include/backend/hashtable.h>

#define LoadFactor 0.75

namespace RawDB {

/**
 * @brief check after insert this record,
 * whether the table need to be expand
 * 
 * @return true if the LoadFactor is greater than 0.75
 * @return false if not
 */
// inline
// bool RawDB::hashTAB::need_more_bucket() {
//     return double(++hdr.rec_num / (hdr.max_bucket + 1)) > LoadFactor;
// }

/* hashTAB类成员函数，根据hashval获得对应的bucketid
 * 将hashval与高位掩码相与，得到尽可能大的bucket
 * 若该bucket_id已大于目前已有的最大bucket_id，则与上低位掩码
 */
// bkt_id RawDB::hashTAB::get_bucket_id(uint32_t hashval) {
//     bkt_id bucket;
//     bucket = hashval & hdr.high_mask;
//     if (bucket > hdr.max_bucket)
//         bucket = bucket & hdr.low_mask;
//     return bucket;
// };  

}


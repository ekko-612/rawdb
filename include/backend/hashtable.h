// hash.h
/* 整个数据库的元数据
 * 数据库最大可占用512TB文件，单个chunk大小2GB，共256K个chunk
 * 其中包含若干个数据库
 */

#pragma once

#include <stdint.h>
#include <string>
#include <seastar/core/distributed.hh>
#include <include/backend/sys_cache.h>
#include <include/stroage/bkt_page.h>

#define LoadFactor 0.75

/* 整个数据库的Metadata
 * 
 */
namespace RawDB {

# define MAX_CHUNK_NUM 262144

struct Metadata{
    long max_seg = 4194304;    //当前seg文件共可容纳的segment数量，若不够则再创建一个seg文件，默认大小2GB（4194304个seg）
    seg_id seg_num = 0;    //segment数量，log2(seg_sz)，默认0
    long seg_sz = 512;    //segment的大小，默认512，其中可存放32个bucket
    long seg_shift = 8;    //segment的偏移量，默认8
    bkt_id max_bucket = 31;    //最大bucket的id，默认初值31
    bkt_id low_mask = 31;    //低位掩码，为不大于max_bucket的最大的2^k再减1，默认31
    bkt_id high_mask = 63;    //高位掩码，为2^(k+1)，默认63
    size_t hashval_sz;    //hash值的大小
    uint32_t hashval_len = 32;    //hashval的长度，默认32
    uint32_t rec_num = 0;    //record的总数量

    Metadata() 
        : max_seg(4194304)
        , seg_num(0)
        , seg_sz(512)
        , seg_shift(8)
        , max_bucket(31)
        , low_mask(31)
        , high_mask(63)
        , hashval_sz()
        , hashval_len(32)
        , rec_num(0)
    {}
    
    Metadata(const Metadata& rbs) 
        : max_seg(rbs.max_seg)
        , seg_num(rbs.seg_num)
        , seg_sz(rbs.seg_sz)
        , seg_shift(rbs.seg_shift)
        , max_bucket(rbs.max_bucket)
        , low_mask(rbs.low_mask)
        , high_mask(rbs.high_mask)
        , hashval_sz(rbs.hashval_sz)
        , hashval_len(rbs.hashval_len)
        , rec_num(rbs.rec_num)
    {}

    Metadata& operator=(const Metadata& rbs) {
        max_seg = rbs.max_seg;
        seg_num = rbs.seg_num;
        seg_sz = rbs.seg_sz;
        seg_shift = rbs.seg_shift;
        max_bucket = rbs.max_bucket;
        low_mask = rbs.low_mask;
        high_mask = rbs.high_mask;
        hashval_sz = rbs.hashval_sz;
        hashval_len = rbs.hashval_len;
        rec_num = rbs.rec_num;
    }
};

/**
 * hashtable's metadata
 * 
 */
class hashTAB {

private:
    Metadata hdr;
    
public:
    hashTAB() = default;

    hashTAB(const hashTAB& rbs) : hdr(rbs.hdr) {};

    hashTAB& operator=(const hashTAB& rbs) {
        hdr = rbs.hdr;
    }
    
    hashTAB(hashTAB&& rbs) = delete;

    bkt_id max_bucket() {
        return hdr.max_bucket;
    }

    bool need_more_bucket()  {
        return double(++hdr.rec_num / (hdr.max_bucket + 1)) > LoadFactor;
    }

    Metadata& get_metadata() {
        return hdr;
    }
    
    bkt_id get_bucket_id(uint32_t hashval)  {
        bkt_id bucket;
        bucket = hashval & hdr.high_mask;
        if (bucket > hdr.max_bucket)
            bucket = bucket & hdr.low_mask;
        return bucket;
    };  
    
private:
    

};


}
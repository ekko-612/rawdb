#pragma once

#include <stdint.h>
#include "../backend/type_define.h"
#include "../backend/hashtable.h"
#include "./disk_manager.h"
#include "../stroage/seg_page.h"
#include "../stroage/bkt_page.h"
#include "../stroage/data_page.h"
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <set>
#include <seastar/core/future.hh>

using namespace seastar;

namespace RawDB {

struct Metadata_cp{
    long max_seg;    //当前seg文件共可容纳的segment数量，若不够则再创建一个seg文件，默认大小2GB（4194304个seg）
    seg_id seg_num;    //segment数量，log2(seg_sz)，默认0
    long seg_sz;    //segment的大小，默认512，其中可存放32个bucket
    long seg_shift;    //segment的偏移量，默认8
    bkt_id max_bucket;    //最大bucket的id，默认初值31
    bkt_id low_mask;    //低位掩码，为不大于max_bucket的最大的2^k再减1，默认31
    bkt_id high_mask;    //高位掩码，为2^(k+1)，默认63
    size_t hashval_sz;    //hash值的大小
    uint32_t hashval_len;    //hashval的长度，默认32
    uint32_t rec_num;    //record的总数量

    Metadata_cp& operator=(const Metadata_cp& rbs) {
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

enum class LogRecordType {
    INVALID = 0,
   
    INSERT,
    DELETE, 
    UPDATE_FOR_DELETE,
    UPDATE_FOR_INSERT,
    
    SPLIT_BEGIN,
    SPLIT_END,
  
    NEW_BUCKET,
    
    NEW_BITMAP,
    
    ALLOCATE_OVERFLOW,
    DEALLOCATE_OVERFLOW,
    
    CHECKPOINT_BEGIN,
    CHECKPOINT_END,
    
};

struct seg_logRecord {
    pg_id seg_page_id;
    uint64_t old_seg_lsn;
    seg_pgHDR seg_pg_header;
    uint16_t bkt_seq;
    Pointer_offset bkt_pos;

    seg_logRecord(pg_id seg_page_id, uint64_t old_seg_lsn, seg_pgHDR seg_pg_header, uint16_t bkt_seq, Pointer_offset bkt_pos)
                : seg_page_id(seg_page_id)
                , old_seg_lsn(old_seg_lsn)
                , seg_pg_header(seg_pg_header)
                , bkt_seq(bkt_seq)
                , bkt_pos(bkt_pos)
    {}
};


struct bkt_logRecord {
    pg_id bkt_page_id;
    uint64_t old_bkt_lsn;
    bkt_pgHDR bkt_pg_header;
    uint16_t bkt_offset;
    hashElement ele;

    bkt_logRecord(pg_id bkt_page_id, uint64_t old_bkt_lsn, bkt_pgHDR bkt_pg_header, uint16_t bkt_offset, hashElement ele)
                : bkt_page_id(bkt_page_id)
                , old_bkt_lsn(old_bkt_lsn)
                , bkt_pg_header(bkt_pg_header)
                , bkt_offset(bkt_offset)
                , ele(ele)
    {}
};

struct LogRecord {
    uint64_t lsn; // 表示日志物理存储的偏移量
    LogRecordType comd;
    
    //metadata
    db_id db;
    tab_id tab;
    Metadata_cp meta;

    //seg
    std::vector<seg_logRecord> seg_log;

    //bkt
    std::vector<bkt_logRecord> bkt_log;

    
    //bitmap
    pg_id bm_page_id;
    //TODO

    //fsm
    //TODO

    //data
    pg_id data_page_id;
    uint64_t old_data_lsn;
    data_pgHDR data_pg_header;
    uint16_t data_offset;
    Item item;
    
    // // split日志，当日志回放的时候，看到这个日志，就进行数据迁移，迁移完毕，再看下一个日志，因为split的时候阻塞写
    // pg_id dir_page_id;
    // bkt_id new_bucket;
    // pg_id new_page_id;
    // pg_id old_page_id;
    
    // // split_end
    // pg_id dir_page_id;
    // pg_id new_page_id;
    // pg_id old_page_id;

    LogRecord(uint64_t lsn, LogRecordType comd, db_id db, tab_id tab, std::string key, std::string val)
        : lsn(lsn)
        , db(db)
        , tab(tab)
        , comd(comd)
        , item(key.size(), val.size(), key.c_str(), val.c_str())
    {}

};

struct LogHeader {
    uint16_t log_len;
    size_t seg_log_num;
    size_t bkt_log_num;
    size_t item_sz;

    LogHeader() = default;

    LogHeader(const LogHeader& rbs)
        : log_len(rbs.log_len)
        , seg_log_num(rbs.seg_log_num)
        , bkt_log_num(rbs.bkt_log_num)
        , item_sz(rbs.item_sz)
    {}

};

struct LogBody {
    uint64_t lsn; // 表示日志物理存储的偏移量
    LogRecordType comd;
    
    //metadata
    db_id db;
    tab_id tab;
    Metadata_cp meta;

    //seg
    seg_logRecord* seg_log;

    //bkt
    bkt_logRecord* bkt_log;

    
    //bitmap
    pg_id bm_page_id;
    //TODO

    //fsm
    //TODO

    //data
    pg_id data_page_id;
    uint64_t old_data_lsn;
    data_pgHDR data_pg_header;
    uint16_t data_offset;
    Item item;

    LogBody(LogRecord rec, size_t seg_log_num, size_t bkt_log_num, size_t item_sz)
        : lsn(rec.lsn)
        , comd(rec.comd)
        , db(rec.db)
        , tab(rec.tab)
        , meta(rec.meta)
        , bm_page_id(rec.bm_page_id)
        , data_page_id(rec.data_page_id)
        , old_data_lsn(rec.old_data_lsn)
        , data_pg_header(rec.data_pg_header)
        , data_offset(rec.data_offset)
        , item(rec.item)
    {
        seg_log = (seg_logRecord*)malloc(seg_log_num * sizeof(seg_logRecord));
        bkt_log = (bkt_logRecord*)malloc(seg_log_num * sizeof(bkt_logRecord));
    }
};

struct Log {
    seastar::promise<bool> finish;
    seastar::promise<int> pro;
    LogHeader _log_hdr;
    LogRecord _log_rec;

    Log(seastar::promise<int> pro, LogHeader& hdr, LogRecord& rec) 
        : pro(std::move(pro))
        , _log_hdr(std::move(hdr))
        , _log_rec(std::move(rec))
    {}

    //不应该存在拷贝构造函数，但是circlular_buffer需要
    Log(const Log& rbs) 
        : _log_hdr(rbs._log_hdr)
        , _log_rec(std::move(rbs._log_rec))
    {}

    Log(Log&& rbs)
        : finish(std::move(rbs.finish))
        , pro(std::move(rbs.pro))
        , _log_hdr(std::move(rbs._log_hdr))
        , _log_rec(std::move(rbs._log_rec))
    {}

};

struct disk_log {
    LogHeader _log_hdr;
    LogBody _log_body;

    disk_log(Log l)
        : _log_hdr(l._log_hdr)
        , _log_body(l._log_rec, l._log_hdr.seg_log_num, l._log_hdr.bkt_log_num,  l._log_hdr.item_sz)
    {}

    disk_log(const disk_log& rbs)
        : _log_hdr(rbs._log_hdr)
        , _log_body(rbs._log_body)
    {}
};


}
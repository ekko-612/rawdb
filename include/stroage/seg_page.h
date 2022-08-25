/*
 * segment_page stored buckets' location
 * 
 */
#pragma once
#include <stdint.h>
#include "bkt_page.h"


namespace RawDB {

static const int seg_size = 8192;
static const int bkt_num_per_seg = seg_size/sizeof(struct Pointer_offset) - 1;
static const int bkt_pointer_size = 16;

#define LocationIndex uint16_t
#pragma pack(1)
struct seg_pgHDR {
    uint64_t pg_lsn = 0;    //page的lsn版本号，page last xlog num
    LocationIndex pg_lower = sizeof(seg_pgHDR);    //空闲空间起始位置
    LocationIndex pg_upper = 8192 - sizeof(seg_pgHDR);    //空闲空间终止位置
    char padding[4];

    seg_pgHDR() : pg_lsn(0), pg_lower(sizeof(seg_pgHDR)), pg_upper(8192 - sizeof(seg_pgHDR)) {}
};


/* hashsegment结构
 * 默认大小8KB，其中可存放511个bucket
 */
struct seg_page {
    struct seg_pgHDR pgHDR;
    Pointer_offset bkt[511];
};
#pragma pack(0)
}
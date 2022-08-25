#pragma once
#include <stdint.h>

namespace RawDB {

#define LocationIndex uint16_t

#pragma pack(1)
/*
 * 这里所有的pointer都可以改进
 * 改成高48bit为page id，低16bit为offset
 * 
 */
struct Pointer_slot {    
    uint64_t page_id;
    uint16_t slot_num;
    char padding[6];

    Pointer_slot() : page_id(0), slot_num(0) {}
    Pointer_slot(uint64_t page_id, uint16_t slot_num) : page_id(page_id), slot_num(slot_num) {}

    Pointer_slot(const Pointer_slot& rbs)
        : page_id(rbs.page_id)
        , slot_num(rbs.slot_num)
    {}

    Pointer_slot& operator=(const Pointer_slot& rbs) {
        page_id = rbs.page_id;
        slot_num = rbs.slot_num;
        return *this;
    }

    Pointer_slot(Pointer_slot&& rbs) noexcept
        : page_id(std::move(rbs.page_id))
        , slot_num(std::move(rbs.slot_num))
    {}

    Pointer_slot& operator=(Pointer_slot&& rbs) {
        page_id = rbs.page_id;
        slot_num = rbs.slot_num;
        return *this;
    }
};

/**
 * @brief size is 16B
 * 
 */
struct Pointer_offset {
    uint64_t page_id;
    uint16_t offset;
    char padding[6];

    Pointer_offset() : page_id(0), offset(0) {}
    Pointer_offset(uint64_t page_id, uint16_t offset) : page_id(page_id), offset(offset) {}

    Pointer_offset(const Pointer_offset& rbs) 
        : page_id(rbs.page_id)
        , offset(rbs.offset)
    {}

    Pointer_offset& operator=(const Pointer_offset& rbs) {
        page_id = rbs.page_id;
        offset = rbs.offset;
        return *this;
    }

    Pointer_offset(Pointer_offset&& rbs) noexcept
        : page_id(std::move(rbs.page_id))
        , offset(std::move(rbs.offset))
    {}
};

/* hash表中每个bucket链表中的结点部分结构，于快速寻找对应的hashvalue结点
 * 不断对比hashvalue，并返回该节点之后的位置。即返回 (char*)(ele) + (sizeof(struct hashElement))
 * flag 表示该结点是否被删除，0表示未删除，1表示已删除
 * item_pos记录了该节点value的pageid与offset偏移量
 * 大小64B
 */
struct hashElement {
    Pointer_slot item_pos;
    Pointer_offset next;
    uint32_t hashval;    //key经过hash函数计算得到的hashvalue
    int flag;
    int encoding;    //value编码方式
    char padding[20];

    hashElement() = default;
    
    hashElement(Pointer_slot item_pos, Pointer_offset next_pos, uint32_t hashval) 
        : item_pos(item_pos)
        , hashval(hashval)
        , next(next_pos)
    {}

    hashElement(Pointer_slot item_pos, Pointer_offset next, uint32_t hashval, int flag, int encoding) 
        : item_pos(item_pos)
        , next(next)
        , hashval(hashval)
        , flag(flag)
        , encoding(encoding)
    {}

    hashElement(const hashElement& rbs) 
        : item_pos(rbs.item_pos)
        , next(rbs.next)
        , hashval(rbs.hashval)
        , flag(rbs.flag)
        , encoding(rbs.encoding)
    {}

    hashElement& operator=(const hashElement& rbs) {
        item_pos = rbs.item_pos;
        next = rbs.next;
        hashval = rbs.hashval;
        flag = rbs.flag;
        encoding = rbs.encoding;
        return *this;
    }
};

/**
 * @brief size is 64B
 * 
 */
struct bkt_pgHDR {
    uint64_t pg_lsn = 0;    //page的lsn版本号，page last xlog num
    LocationIndex pg_lower = sizeof(bkt_pgHDR);    //空闲空间起始位置，即第几个ele是空闲的
    LocationIndex pg_upper = 8192 - sizeof(bkt_pgHDR);    //空闲空间终止位置
    uint64_t crc;
    char padding[44];

    bkt_pgHDR() : pg_lsn(0), pg_lower(sizeof(bkt_pgHDR)), pg_upper(8192 - sizeof(bkt_pgHDR)) {}
};

struct bkt_page {
    struct bkt_pgHDR pgHDR;
    struct hashElement ele[127];
};

#pragma pack(0)

}
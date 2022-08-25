#pragma once
#include <stdint.h>
#include <cstddef>

namespace RawDB {

using LocationIndex = uint16_t;

#pragma pack(1)
/* 每个page头部控制结构
 * pg_lsn 记录该page的最后更新的lsn编号，以便根据log进行更新
 * pg_lower 记录该页中空闲空间的起始位置
 * pg_upper 记录该页中空闲空间的终止位置
 * 
 * bitmap记录哪些slot已被使用
 * 
 * slot 存储每个Item的起始位置
 * 
 * 大小为54B
 */
struct data_pgHDR {
	uint64_t pg_lsn;
	char bitmap[32];
	uint16_t slot[256];
	LocationIndex pg_lower;
	LocationIndex pg_upper;
	uint64_t crc;

	data_pgHDR() : pg_lsn(0), pg_lower(sizeof(data_pgHDR)), pg_upper(8192)
	{}

	data_pgHDR(const data_pgHDR& rbs) 
		: pg_lsn(rbs.pg_lsn)
		, pg_lower(rbs.pg_lower)
		, pg_upper(rbs.pg_lower)
		, crc(rbs.crc)
	{
		memcpy(bitmap, rbs.bitmap, 32 * sizeof(char));
		memcpy(slot, rbs.slot, 256 * sizeof(uint16_t));
	}
};

static const int data_page_header_size = sizeof(data_pgHDR);

struct Item_len {
	size_t key_len;
    size_t val_len;

	Item_len() = default;

	Item_len(size_t klen, size_t vlen)
		: key_len(klen)
		, val_len(vlen)
	{}

	Item_len(const Item_len& rbs)
		: key_len(rbs.key_len)
		, val_len(rbs.val_len)
	{}

	Item_len& operator=(const Item_len& rbs) {
		key_len = rbs.key_len;
		val_len = rbs.val_len;
	}
};

/* 单个KV数据的具体值
 * val_len 记录该value的实际长度
 * key[32] 记录key的值
 * val 指针记录value起始地址 
 */
struct Item {
	Item_len len;
    char* key;    //key
    char* val;    //value

	//Item() = default;

	Item(size_t klen, size_t vlen, const char* k, const char* v) 
		: len(klen, vlen)
	{
		key = (char*)malloc(klen);
		val = (char*)malloc(vlen);
		//用memcpy会有问题，结尾多一个`
		strcpy(key, k);
		strcpy(val, v);
	}

	Item(const Item& rbs) 
		: len(rbs.len)
	{
		key = (char*)malloc(rbs.len.key_len);
		val = (char*)malloc(rbs.len.val_len);
		strcpy(key, rbs.key);
		strcpy(val, rbs.val);
	}

	Item& operator=(const Item& rbs) {
		len = rbs.len;
		key = (char*)malloc(rbs.len.key_len);
		val = (char*)malloc(rbs.len.val_len);
		strcpy(key, rbs.key);
		strcpy(val, rbs.val);
	}
};

struct data_page {
	struct data_pgHDR pgHDR;
	char data[ 8192 - data_page_header_size ];

};
#pragma pack(0)

}
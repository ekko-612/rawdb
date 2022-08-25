#pragma once


#include <include/backend/type_define.h>
#include <stdint.h>
/*  格式 (size in bytes):
 *  ----------------------------------------------------------------
 *  | CRC(8) | PageId (8)| LSN (8)| empty_cnt (2) | bitmap (256MB)
 *  ----------------------------------------------------------------
 */
/*
分组，每组256MB,每组都要一个bitmap来管理对应page的分配和回收。

1个page可以管理1组(256MB)。4TB大概需要8192*2页来管理。

同时为了加快访问，每次启动时，扫描所有组，并将每一组中空闲page的个数（empty_cnt）存到内存中。每次分配回收page时，修改empty_cnt和bitmap的同时也会在内存中维护empty_cnt。
大概占用空间：4TB可以分为2^14组，需要2^14 * 2B = 32KB内存。
请求新的一页时，先访问内存，查看哪些组有空闲page，再去访问对应的组的bitmap，找到一个空闲page。也需要写WAL日志，写到哈希表部分那里。
*/
namespace RawDB {

class Page{

protected:
    char data[8192]; // 8192

private:
    bool is_dirty;
    seastar::rwlock write_mut;
    
    
public:
    Page() {
        is_dirty = false;
        write_mut.for_write();
        memset(data, 0, sizeof(data));
    }

    explicit Page(const char* data) {
        is_dirty = false;
        write_mut.for_write();
        memcpy(this->data, data, sizeof(this->data));
    }

    void ResetMemory() {
        memset(data, 0, sizeof(data));
    }

    char* GetData() {
        return data;
    }

    void SetCRC(uint64_t crc) {
        for (uint16_t i = 0; i < 8; i++) {
            data[OFFSET_CRC + i] = crc & 0xFF;
            crc >>= 8;
        }
    }

    uint64_t GetCRC() {
        uint64_t crc = 0;
        for (uint16_t i = 0; i < 8; i++) {
            crc |= data[OFFSET_CRC + 7 - i];
            crc <<= 8;
        }
        return crc;
    }

    pg_id GetPageId() {
        pg_id id = 0;
        for (uint16_t i = 0; i < 8; i++) {
            id |= data[OFFSET_PAGE_ID + 7 - i];
            id <<= 8;
        }
        return id;
    }

    lsn_t GetLsn() {
        lsn_t lsn = 0;
        for (uint16_t i = 0; i < 8; i++) {
            lsn |= data[OFFSET_LSN + 7 - i];
            lsn <<= 8;
        }
        return lsn;
    }

    void SetLsn(lsn_t lsn) {
        for (uint16_t i = 0; i < 8; i++) {
            data[OFFSET_LSN + i] = lsn & 0xFF;
            lsn >>= 8;
        }
    }

    bool IsDirty() {
        return is_dirty;
    }

    void SetDirty() {
        is_dirty = true;
    }

    future<> Lock() {
        return write_mut.write_lock();
    }
    
    void Unlock() {
        write_mut.write_unlock();
    }
    
    static constexpr size_t OFFSET_CRC = 0;
    static constexpr size_t OFFSET_PAGE_ID = 8;
    static constexpr size_t OFFSET_LSN = 16;
};

class BitmapPage : public Page {
private:
    void SubEmptyCnt() {
        uint16_t empty_cnt = GetEmptyCnt();
        empty_cnt--;
        data[OFFSET_EMPTY_CNT] = empty_cnt & 0xFF;
        data[OFFSET_EMPTY_CNT + 1] = (empty_cnt >> 8) & 0xFF;
    }

public:
    BitmapPage() : Page() {
        uint16_t empty_cnt = 0xFFFF;
        data[OFFSET_EMPTY_CNT] = empty_cnt & 0xFF;
        data[OFFSET_EMPTY_CNT + 1] = (empty_cnt >> 8) & 0xFF;
    }

    explicit BitmapPage(const char* data) : Page(data) { }

    pg_id Allocate() {
        for (uint16_t offset = 26; offset < 8192; offset++)
            if (data[offset] != 0xFF)
                for (uint8_t num = 0; num < 8; num++)
                    if (!(data[offset] & (1 << num))) {
                        Occupy(offset, num);
                        return (((pg_id)offset) << 8) | num;
                    }
    }

    void Occupy(uint16_t offset, uint8_t num) { // 第offset个字节，第num个bit
        if (offset >= 8192 || offset < 26 || num >= 8) {
            fprintf(stderr, "Out of index\n");
            return;
        }
        data[offset] |= (1 << num);
        SubEmptyCnt();
    }

    void Release(uint16_t offset, uint8_t num) {
        if (offset >= 8192 || num >= 8) {
            fprintf(stderr, "Out of index\n");
            return;
        }
        data[offset] &= (~(1 << num));
        AddEmptyCnt();
    }

    uint16_t GetEmptyCnt() {
        uint16_t empty_cnt = 0;
        empty_cnt |= data[OFFSET_EMPTY_CNT + 1];
        empty_cnt <<= 8;
        empty_cnt |= data[OFFSET_EMPTY_CNT];
        return empty_cnt;
    }

    void AddEmptyCnt() {
        uint16_t empty_cnt = GetEmptyCnt();
        empty_cnt++;
        data[OFFSET_EMPTY_CNT] = empty_cnt & 0xFF;
        data[OFFSET_EMPTY_CNT] = (empty_cnt >> 8) & 0xFF;
    }

    static constexpr size_t OFFSET_EMPTY_CNT = 24;
};

}
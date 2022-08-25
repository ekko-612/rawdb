#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <include/manager/log.h>
#include <include/manager/disk_manager.h>
#include <include/manager/log_stream.h>
#include <include/stroage/bitmap_page.h>

namespace RawDB {



class PageAllocateManager {
private:
    std::string db;
    DiskManager* disk_manager;
   // LogManager* log_manager;
   //uint16_t index_page[8192];

public:
    PageAllocateManager() = default;

    future<pg_id> ReturnPage(std::string db_id, uint32_t tab_id, std::string fi) {
        return make_ready_future<pg_id>(0);
    }

    // PageAllocateManager(std::string db, DiskManager *disk_manager, LogManager *log_manager)
    //     : db(db), disk_manager(disk_manager), log_manager(log_manager) { 
    //     disk_manager->create_db(db);
    //     for (tab_id tab; tab < 8192; tab++) {
    //         // 如何读取第tab个bitmap？uint32_t table_id, sstring fi, pg_id page_id 这三个参数应该为多少？
    //         temporary_buffer<char> buffer = disk_manager->ReadPage(db, ?, ?, ?);
    //         BitmapPage bitmap_page(buffer.get());
    //         index_page[tab] = bitmap_page[tab]->GetEmptyCnt();
    //     }
    // }

    // future<> NewPage(pg_id& page_id) {
    //     for (tab_id tab; tab < 8192; tab++) {
    //         if (index_page[tab]) {
    //             temporary_buffer<char> buffer = disk_manager->ReadPage(db, ?, ?, ?);
    //             BitmapPage bitmap_page(buffer.get());
    //             bitmap_page.Lock();
    //             page_id = bitmap_page.Allocate();
    //             bitmap_page.Unlock();
    //             return disk_manager->WritePage(db, ?, ?, ?);
    //         }
    //     }
    //     fprintf(stderr, "空间已满\n");
    //     return make_ready_future<>();
    // }

    // future<pg_id> ReturnPage(db_id db, tab_id tab) {
    //     //TODO
    //     return disk_manager->ReadPage(db, tab, "bmp", 0).then([=] (temporary_buffer<char> buffer) {
    //         BitmapPage bitmap_page(buffer.get());
    //         bitmap_page.Lock();
    //         bitmap_page.Release(page_id >> 8, page_id & 0xFF);
    //         bitmap_page.Unlock();
    //         index_page[tab] = bitmap_page.GetEmptyCnt();
    //         return disk_manager->WritePage(db, ?, ?, ?);
    //      });
  
    // }

    // future<pg_id> ReturnPage(tab_id tab, pg_id page_id) {
    //     //TODO
    //     temporary_buffer<char> buffer = disk_manager->ReadPage(db, ?, ?, ?);
    //     BitmapPage bitmap_page(buffer.get());
    //     bitmap_page.Lock();
    //     bitmap_page.Release(page_id >> 8, page_id & 0xFF);
    //     bitmap_page.Unlock();
    //     index_page[tab] = bitmap_page.GetEmptyCnt();
    //     return disk_manager->WritePage(db, ?, ?, ?);
    // }

    static constexpr uint16_t GROUP_SIZE = 10;
};

}
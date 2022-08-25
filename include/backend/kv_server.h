#pragma once

#include <iostream>
#include <regex>
#include <include/backend/type_define.h>
#include <include/backend/sys_cache.h>
#include <include/backend/hashtable.h>
#include <include/backend/meta.h>
#include <include/stroage/data_page.h>
#include <include/stroage/seg_page.h>
#include <include/stroage/bkt_page.h>
#include <include/manager/disk_manager.h>
#include <include/manager/bitmap.h>
#include <include/manager/log.h>
#include <seastar/core/semaphore.hh>
#include <seastar/core/circular_buffer.hh>

namespace RawDB {

static thread_local seastar::semaphore limit(1);
    
// 解析pg时使用的wrapper
template <typename T>
class page_object_wrapper{
public: 
    using pointer = T*;    //定义模板类型指针为pointer
private: 
    seastar::temporary_buffer<char> _buf;
    pointer _obj;
       
public:
    page_object_wrapper(seastar::temporary_buffer<char>&& _buf) : _buf(std::move(_buf)) 
    {};

    pointer operator->() {
        return reinterpret_cast<pointer>((_buf).get_write());
    }

    //将传入的新temporary_buffer替换原来的temporary_buffer
    void reset(seastar::temporary_buffer<char>&& buf) {
        _buf = std::move(buf);
    }

    T get_it() {
        return *(reinterpret_cast<pointer>((_buf).get_write()));
    }

    char* get_write() {
        return _buf.get_write();
    }
};

/**
 * @brief  一个core上一个kv_server对象，其拥有独立的sys_cache及若干table对象（默认100)
 * 
 * _htb stores many table, which belong to many db
 *  its key is db_id + table_id, value is the table's metadata
 */
class kv_server {
public:
    static const int htbnum_per_db_per_core = 100;
    uint64_t _lsn;
    uint64_t _flushed_lsn;
    
private:
    std::unordered_map<std::string, hashTAB> _htb;
    sys_cache cache;
    DiskManager _disk;
    PageAllocateManager _bitmap;
    //LogManager log_mgr;
    seastar::circular_buffer<Log> _log_buffer;

public:
    kv_server() = default;
    
    kv_server(uint64_t per_cpu_slab_size, uint64_t slab_page_size)
    {
        using namespace std::chrono;
        using clock_type = seastar::lowres_clock;
        clock_type::duration _wc_to_clock_type_delta =
            duration_cast<clock_type::duration>(clock_type::now().time_since_epoch() - system_clock::now().time_since_epoch());

        // initialize per-thread slab allocator.
//         slab_holder = std::make_unique<slab_allocator<item>>(default_slab_growth_factor, per_cpu_slab_size, slab_page_size,
//                 [this](item& item_ref) { erase<true, true, false>(item_ref); _stats._evicted++; });
//         slab = slab_holder.get();
// #ifdef __DEBUG__
//         static bool print_slab_classes = true;
//         if (print_slab_classes) {
//             print_slab_classes = false;
//             slab->print_slab_classes();
//         }
// #endif
    }

    sys_cache& get_cache() {
        return cache;
    }

    /**
     * @brief load all table metadata belongs to this core
     * use this_shard_id() to get this core id
     * 
     * total rawdb's metadata is at BASE_DIR/data/meta 
     * every db has 100(htbnum_per_db_per_core) table
     * every table's metadata is at seg_page 0
     * 
     */
    
    future<> init() {
        std::cout << "init begin" << std::endl;
 
       // std::cout << "read meta begin" << std::endl;
        //return _disk.ReadMeta().then([=] (seastar::temporary_buffer<char> _buf) {
        return _disk.ReadPage("0",0,"meta",0).then([=] (seastar::temporary_buffer<char> _buf) {
            page_object_wrapper<metadata> m(_buf.share(0, sizeof(Metadata)));
            //std::cout << "he" << std::endl;
            return seastar::do_with(int(m->db_num), int(100 / smp::count), int(1), int(1), int(10), [&] (auto& db_num, auto& table_num_per_core, auto& cur_db, auto& cur_table, auto& last_table) {
            // return seastar::do_with(int(m->db_num), int(htbnum_per_db_per_core / smp::count), int(1), int(1), int(100), [&] (auto& db_num, auto& table_num_per_core, auto& cur_db, auto& cur_table, auto& last_table) {
                //std::cout << "loop begin" << std::endl;
                return seastar::do_until([&] { return cur_db > db_num; }, [&] {
                    
                    cur_table = table_num_per_core * this_shard_id() + 1;
                    last_table = table_num_per_core * (this_shard_id() + 1);
                    //std::cout << "begin at:" << cur_table << " end at:" << last_table << std::endl;
                    return seastar::do_until([&] { return cur_table > last_table; }, [&] {
                        return _disk.ReadPage(std::to_string(cur_db), cur_table, "meta", 0).then([=, &cur_db, &cur_table] (seastar::temporary_buffer<char> buf){
                            //std::cout << "begin insert" << std::endl;
                            //std::cout << cur_db << " " << cur_table << std::endl;
                            page_object_wrapper<hashTAB> wrapper(std::move(buf.share(0, sizeof(hashTAB))));
                            hashTAB tb(wrapper.get_it());
                            _htb.insert({std::string(std::to_string(cur_db) + " " + std::to_string(cur_table)), tb});
                            ++cur_table;
                        });
                    }).then([&] {
                        ++cur_db;
                    });
                });
            });
        }).then([&] {
            std::cout << "init finish" << std::endl;
        });
    }

    
    std::string test_get_table_dir() {
        std::cout << "dir is ";
        std::cout << _disk.get_table_dir("0", 2) << std::endl;
        std::pair<std::string, uint32_t> p = _disk.gen_filename_and_offset("0", 2, "meta", 0);
        std::cout << p.first <<  " " << p.second << std::endl;
    }

    future<> test_CreateDB() {
        return _disk.CreateDB("0").then([&] {
            std::cout << "CreateDB ok!!!" << std::endl;
        });
    }

    /**
     * @brief command add, insert a key-value to a specified db
     * check whether the db exists
     * if exists update metadata and then check the key whether is exists in the db
     *  if exitst, return false
     *  if not, insert it into the table and then write log and flush
     *      to disk and return true
     * 
     * @param _insertion 
     * @return true if insert successfully
     * @return false if failed
     */
    future<bool> add(item_insertion_data _insertion) {
        
        // using namespace std;//test
        //cache.add_page();
        // return seastar::do_with(std::move(_insertion), [this] (auto& _insertion) {
        //     return get_ele_data_pos(_insertion.db_id, _insertion.tab_id, Pointer_offset(0, sizeof(bkt_pgHDR))).then([this, &_insertion] (Pointer_slot pos) {
        //         return get_value(_insertion.db_id, _insertion.tab_id, pos).then([] (std::string val) {
        //             std::cout << val << std::endl;
        //         }).then([] {
        //             return true;
        //         });
        //     });
        // });
        
        
        // seastar::temporary_buffer<char> bu(8192);
        // insert_page_to_cache("test", std::move(bu));
        // return seastar::do_with(std::move(_insertion), [this] (auto& _insertion) {
        //     return exist_key(_insertion, 0);
        // });
        // cache.print_page();
        // std::cout << "begin" << std::endl;
        // return get_last_ele_pos("0", 0, 0).then([this] (Pointer_offset pos) {
        //     std::cout << pos.page_id << " " << pos.offset << std::endl;
        //     return true;
        // });


//below is true version
        std::string tab = _insertion.db_id + " " + std::to_string(_insertion.tab_id);
        auto it = _htb.find(tab);
        //if not find the table
        if(it == _htb.end()) {
            //std::cout << "not fon? " << tab << "  ! " << _htb.size() << std::endl;
            return make_ready_future<bool>(false);
        }
            
        Log& log = create_log(_insertion);
        return seastar::do_with(std::move(_insertion), std::move(it), bkt_id(), [this, &log, &loghdr = log._log_hdr, &logrec = log._log_rec] (auto& _insertion, auto& it, auto& bkt) {
            //compute the target bkt id
            bkt = get_bkt_id(_insertion.key.hash(), it->second);
            return exist_key(_insertion, bkt).then([this, &_insertion, &it, &bkt, &log, &logrec, &loghdr] (Pointer_offset pos) {
                //if the key already exists
                if(pos.offset != 0) {
     //std::cout << "key exists!" << std::endl;
                    return make_ready_future<bool>(false);
                }
                    
                return expand_table(_insertion.db_id, _insertion.tab_id, it->second, loghdr, logrec).then([this, &_insertion, &it, &bkt, &log, &logrec, &loghdr] () {
        //  std::cout << "expand_table finish!" << std::endl;
                    //get the inserted item positon
                    return insert_data_item(_insertion, loghdr, logrec).then([this, &_insertion, &it, &bkt, &log, &logrec, &loghdr] (Pointer_slot item_pos) {
                        //get the first hashelement pos in this bkt
        std::cout << "insert data finish! item pos is:" << item_pos.slot_num << std::endl;
                        return get_first_ele_pos(_insertion.db_id, _insertion.tab_id, bkt).then([this, &_insertion, &it, &bkt, item_pos, &log, &logrec, &loghdr] (Pointer_offset first_ele_pos) {
       //   std::cout << "get ele finish!" << std::endl;
                            //get the new hashelement pos
                            return insert_ele_item(_insertion.db_id, _insertion.tab_id, _insertion.key.hash(), first_ele_pos, item_pos, loghdr, logrec).then([this, &_insertion, bkt, &log, &logrec, &loghdr] (Pointer_offset pos) {
            // std::cout << "insert ele finish!" << std::endl;
                                return update_first_ele_pos(_insertion.db_id, _insertion.tab_id, bkt, pos, loghdr, logrec).then([this, &log, &loghdr, &logrec] () {
                                    // log.finish.set_value(true);
                                    // flush_log();
                                    // auto f = log.pro.get_future();
                                    // f.get();
                                    return true;
                                });
                            });
                        });
                    });
                });
            });
        });
    }

    future<bool> replace(item_insertion_data _insertion) {
        //TODO
    }

    future<bool> remove(item_insertion_data item) {
        //TODO
    }

    future<std::string> get(item_insertion_data item) {
        std::string tab = item.db_id + " " + std::to_string(item.tab_id);
        auto it = _htb.find(tab);
        //if not find the table
        if(it == _htb.end())
            return make_ready_future<std::string>("not found db");
        return seastar::do_with(std::move(item), bkt_id(), [this, it] (auto& item, auto& bkt) {
            //compute the target bkt id
            bkt = get_bkt_id(item.key.hash(), it->second);
            return exist_key(item, bkt).then([this, &item, &bkt] (Pointer_offset pos) {
                //if the key not exists
                if(pos.offset == 0)
                    return make_ready_future<std::string>("not found key");
                return get_ele_data_pos(item.db_id, item.tab_id, pos).then([this, &item, &bkt] (Pointer_slot pos) {
                    return get_value(item.db_id, item.tab_id, pos);
                });
                
            });
        });
    }

    future<> stop() {
        //这里需要将所有脏页刷脏
        return make_ready_future<>(); 
    }

private:

    /**
     * @brief judge the key whether exist in the table
     * 
     * @param _insertion 
     * @return its pos if exist
     * @return offset = 0 if not exist
     */
    future<Pointer_offset> exist_key(item_insertion_data& _insertion, bkt_id bkt) {

        return seastar::do_with(Pointer_offset(), bool(), bool(), [this, bkt, &_insertion] (auto& cur_pos, auto& find_key, auto& stop_loop) {
  //std::cout << "_inser_hashval:" << _insertion.key.hash() << std::endl;
            find_key = false;
            stop_loop = false;
            return get_first_ele_pos(_insertion.db_id, _insertion.tab_id, bkt).then([this, &_insertion, bkt, &cur_pos, &find_key, &stop_loop] (Pointer_offset first_pos) {
    //std::cout << "first ele: " << first_pos.offset << std::endl;
                if(first_pos.offset == 0)
                    return make_ready_future<Pointer_offset>(first_pos);
                cur_pos = first_pos;
                //如果可以直接跳出do_until循环会写的更好看一些
                return seastar::do_until([&stop_loop] { return stop_loop; }, [this, &_insertion, bkt, &cur_pos, &find_key, &stop_loop] {
                    return get_ele_hashval(_insertion.db_id, _insertion.tab_id, cur_pos).then([this, &cur_pos, &_insertion, &find_key, &stop_loop] (uint32_t hashval) {
      //std::cout << "compare:" << cur_pos.page_id<<" "<<cur_pos.offset << " hashval:" << hashval<< " " << _insertion.key.hash() <<std::endl;
                        if(hashval == _insertion.key.hash()) {
                            //if find the key, then stop the loop and return result
  //std::cout << "find!!!!!!!!!!!" << std::endl;
                            find_key = true;
                            stop_loop = true;
                            return make_ready_future<>();
                        }
                        return get_next_ele_pos(_insertion.db_id, _insertion.tab_id, cur_pos).then([this, &cur_pos, &stop_loop] (Pointer_offset pos) {
      //std::cout << "not find!!!!!!!!!!!" << std::endl;
                            if(pos.offset == 0) {
                                stop_loop = true;
                                return make_ready_future<>();
                            }
                            cur_pos = pos;
                            return make_ready_future<>();
                        });
                    });
                }).then([&find_key, &cur_pos] {
                    if(find_key) {
      //std::cout << "return curpos:" << cur_pos.offset << std::endl;
                        return cur_pos;
                    }
                    return Pointer_offset(0, 0);
                });
            });
        });

    }


    // /**
    //  * @brief judge the key whether exist in the table
    //  * 
    //  * @param _insertion 
    //  * @return true if exist
    //  * @return false if not exist
    //  */
    // future<bool> exist_key(item_insertion_data& _insertion, bkt_id bkt) {

    //     return seastar::do_with(Pointer_offset(), bool(), bool(), [this, bkt, &_insertion] (auto& cur_pos, auto& find_key, auto& stop_loop) {
    //         find_key = false;
    //         stop_loop = false;
    //         return get_first_ele_pos(_insertion.db_id, _insertion.tab_id, bkt).then([this, &_insertion, bkt, &cur_pos, &find_key, &stop_loop] (Pointer_offset first_pos) {
    //             if(first_pos.offset == 0)
    //                 return make_ready_future<bool>(false);
    //             cur_pos = first_pos;
    //             //如果可以直接跳出do_until循环会写的更好看一些
    //             return seastar::do_until([&stop_loop] { return stop_loop; }, [this, &_insertion, bkt, &cur_pos, &find_key, &stop_loop] {
    //                 return get_ele_hashval(_insertion.db_id, _insertion.tab_id, cur_pos).then([this, &cur_pos, &_insertion, &find_key, &stop_loop] (uint32_t hashval) {
    //                     //std::cout << "compare:" << cur_pos.page_id<<" "<<cur_pos.offset << " hashval:" << hashval <<std::endl;
    //                     if(hashval == _insertion.key.hash()) {
    //                         //if find the key, then stop the loop and return result
    //                         find_key = true;
    //                         stop_loop = true;
    //                         return make_ready_future<>();
    //                     }
    //                     return get_next_ele_pos(_insertion.db_id, _insertion.tab_id, cur_pos).then([this, &cur_pos, &stop_loop] (Pointer_offset pos) {
    //                         if(pos.offset == 0) {
    //                             stop_loop = true;
    //                             return make_ready_future<>();
    //                         }
    //                         cur_pos = pos;
    //                         return make_ready_future<>();
    //                     });
    //                 });
    //             }).then([&find_key] {
    //                 return find_key;
    //             });
    //         });
    //     });

    // }

    future<Pointer_slot> get_ele_data_pos(const std::string db_id
                            , const uint32_t tab_id
                            , Pointer_offset pos) {
        return seastar::do_with(get_bkt_page_name(db_id, tab_id, pos.page_id), [=] (auto& pg) {
            return prepare_page_for_read(pg).then([=, &pg] (seastar::temporary_buffer<char> buf) {
                page_object_wrapper<hashElement> wrapper(buf.share(pos.offset, sizeof(hashElement)));
                Pointer_slot item_pos = wrapper->item_pos;
                finish_page_for_read(pg);
                return item_pos;
            });
        });
    }

    future<std::string> get_value(const std::string db_id
                            , const uint32_t tab_id
                            , Pointer_slot pos) {

        return seastar::do_with(get_data_page_name(db_id, tab_id, pos.page_id), [=] (auto& pg) {
            return prepare_page_for_read(pg).then([=] (seastar::temporary_buffer<char> buf) {
                page_object_wrapper<data_pgHDR> hdr(buf.share(0, sizeof(data_pgHDR)));
                uint16_t offset = hdr->slot[pos.slot_num];
                page_object_wrapper<Item_len> len(buf.share(offset, sizeof(Item_len)));
 std::cout << "len is " << len->key_len << " " <<  len->val_len << std::endl;
                page_object_wrapper<Item> item(buf.share(offset, sizeof(Item_len) + len->key_len + len->val_len));
                std::string val = item->val;
                finish_page_for_read(pg);
                return val;
            });
        });
    }

    future<uint32_t> get_ele_hashval(const std::string db_id
                                , const uint32_t tab_id
                                , Pointer_offset pos) {

        //get page
        return prepare_page_for_read(get_bkt_page_name(db_id, tab_id, pos.page_id)).then([this, db_id, tab_id, pos] (seastar::temporary_buffer<char> bkt_pg) {
            //get hashelement obj, at the para pos
            //page_object_wrapper<hashElement> hash_ele(bkt_pg.share(pos.offset, sizeof(hashElement)));
            page_object_wrapper<bkt_page> bk(bkt_pg.share());
            uint32_t hashval = bk->ele[pos.offset / sizeof(hashElement)].hashval;
//std::cout << "hashv: " << hashval << " " << bk->ele[pos.offset / sizeof(hashElement)].hashval << std::endl;
            finish_page_for_read(get_bkt_page_name(db_id, tab_id, pos.page_id));
            
            return hashval;
        });             
    }

    /**
     * @brief Get the first ele pos object
     * 
     * @param db_id 
     * @param tab_id 
     * @param bkt 
     * @return Pointer_offset , the position pointer of the ele
     */
    future<Pointer_offset> get_first_ele_pos(const std::string db_id
                                    , const uint32_t tab_id
                                    , const bkt_id bkt) {
                                        std::cout << "bkt: " << bkt << "page:" << bkt_num_per_seg << " " << (bkt / bkt_num_per_seg) << std::endl;
        std::string pg = get_seg_page_name(db_id, tab_id, bkt / bkt_num_per_seg);
        //get page
        return prepare_page_for_read(pg).then([this, bkt, pg] (seastar::temporary_buffer<char> seg_pg) {
            //get obj, this bkt_pos is the first ele's in this bkt
            page_object_wrapper<seg_page> segment_page(seg_pg.share());
            Pointer_offset ele_pos = segment_page->bkt[bkt % bkt_num_per_seg];

            finish_page_for_read(pg);
            std::cout << ele_pos.page_id << "first" << ele_pos.offset << std::endl;
            return ele_pos;
        });
    }

    future<Pointer_offset> get_next_ele_pos(const std::string db_id
                                    , const uint32_t tab_id
                                    , Pointer_offset pos) {                     
        //get page
        return prepare_page_for_read(get_bkt_page_name(db_id, tab_id, pos.page_id)).then([this, db_id, tab_id, pos] (seastar::temporary_buffer<char> bkt_pg) {
            //get hashelement obj, at the para pos
            page_object_wrapper<hashElement> hash_ele(bkt_pg.share(pos.offset, sizeof(hashElement)));
            Pointer_offset next_ele_pos = hash_ele->next;
            finish_page_for_read(get_bkt_page_name(db_id, tab_id, pos.page_id));
std::cout << next_ele_pos.page_id << "next" << next_ele_pos.offset << std::endl;
            return next_ele_pos;
        });
    }

    /**
     * @brief Get the last ele pos in the bucket
     * 
     * @param db_id 
     * @param tab_id 
     * @param bkt 
     * @return Pointer_offset the last ele pos
     */
    future<RawDB::Pointer_offset> get_last_ele_pos(const std::string db_id
                                , const uint32_t tab_id
                                , const bkt_id bkt) {
        return seastar::do_with(Pointer_offset(), Pointer_offset(), [this, db_id, tab_id, bkt] (auto& cur_pos, auto& next_pos) {
            return get_first_ele_pos(db_id, tab_id, bkt).then([this, db_id, tab_id, bkt, &cur_pos, &next_pos] (Pointer_offset first_pos) {
                cur_pos = first_pos;
                next_pos = first_pos;
                //如果可以直接跳出do_until循环会写的更好看一些
                return seastar::do_until([&next_pos] { return next_pos.offset == 0; }, [this, db_id, tab_id, bkt, &cur_pos, &next_pos] {
                    return get_next_ele_pos(db_id, tab_id, next_pos).then([this, &cur_pos, &next_pos] (Pointer_offset pos) {
                        cur_pos = next_pos;
                        next_pos = pos;
                    });
                }).then([&cur_pos] {
                    return cur_pos;
                });
            });
        });
        
    }

    /**
     * @brief update the first ele position in the bucket
     * 
     * @param db_id 
     * @param tab_id 
     * @param bkt 
     * @param pos 
     * @return future<> 
     */
    future<> update_first_ele_pos(const std::string db_id
                                , const uint32_t tab_id
                                , const bkt_id bkt
                                , Pointer_offset pos
                                , LogHeader& loghdr
                                , LogRecord& logrec) {
        return prepare_page_for_write(get_seg_page_name(db_id, tab_id, bkt / bkt_num_per_seg)).then([this, db_id, tab_id, bkt, pos, &loghdr, &logrec] (seastar::temporary_buffer<char> seg_page) {
            page_object_wrapper<RawDB::seg_page> wrapper(std::move(seg_page.share()));
std::cout << "update first pos is " << pos.offset << std::endl;
            wrapper->bkt[bkt % bkt_num_per_seg] = pos;

            //update log
            ++loghdr.seg_log_num;
            page_object_wrapper<RawDB::seg_pgHDR> hdr(std::move(seg_page.share(0, sizeof(seg_pgHDR))));
            logrec.seg_log.push_back(seg_logRecord(bkt / bkt_num_per_seg, hdr->pg_lsn, hdr.get_it(), bkt % bkt_num_per_seg, pos));

            //update bkt page lsn
            hdr->pg_lsn = logrec.lsn;

            finish_page_for_write(get_seg_page_name(db_id, tab_id, bkt / bkt_num_per_seg));
        });
    }

    future<> update_ele_next_pos(const std::string db_id
                                , const uint32_t tab_id
                                , Pointer_offset pos
                                , Pointer_offset next_pos
                                , LogHeader& loghdr
                                , LogRecord& logrec) {
        return prepare_page_for_write(get_bkt_page_name(db_id, tab_id, pos.page_id)).then([this, db_id, tab_id, pos, next_pos, &loghdr, &logrec] (seastar::temporary_buffer<char> ele) {
            page_object_wrapper<RawDB::hashElement> wrapper(std::move(ele.share(pos.offset, sizeof(RawDB::hashElement))));
            wrapper->next = next_pos;
            
            //update log
            ++loghdr.bkt_log_num;
            page_object_wrapper<RawDB::bkt_pgHDR> hdr(std::move(ele.share(0, sizeof(bkt_pgHDR))));
            logrec.bkt_log.push_back(bkt_logRecord(pos.page_id, hdr->pg_lsn, hdr.get_it(), pos.offset, wrapper.get_it()));

            //update bkt page lsn
            hdr->pg_lsn = logrec.lsn;

            finish_page_for_write((get_bkt_page_name(db_id, tab_id, pos.page_id)));
        });
    }

    /**
     * @brief insert a hashelement in the bkt
     * and update the first hashelement pos in the bkt
     * 
     * @param db_id 
     * @param tab_id 
     * @param hashval 
     * @param data_pos 
     * @return future<Pointer_offset> 
     */
    future<Pointer_offset> insert_ele_item(const std::string db_id
                                    , const uint32_t tab_id
                                    , const uint32_t hashval
                                    , Pointer_offset next_pos
                                    , Pointer_slot data_pos
                                    , LogHeader& loghdr
                                    , LogRecord& logrec) {
        
        return seastar::do_with(pg_id(), std::string(), [this, db_id, tab_id, next_pos, data_pos, hashval, &loghdr, &logrec] (auto& page_id, auto& pg) {
            return _bitmap.ReturnPage(db_id, tab_id, "bkt").then([this, db_id, tab_id, next_pos, data_pos, hashval, &page_id, &pg, &loghdr, &logrec] (pg_id id) {
                page_id = id;
                pg = get_bkt_page_name(db_id, tab_id, page_id);
                return prepare_page_for_write(pg).then([this, db_id, tab_id, next_pos, data_pos, hashval, &page_id, &pg, &loghdr, &logrec] (seastar::temporary_buffer<char> bkt_pg) {
                    page_object_wrapper<bkt_page> bucket_page(bkt_pg.share());
                    pg_offset offset = bucket_page->pgHDR.pg_lower;
    
                    bucket_page->ele[offset / sizeof(hashElement)] = hashElement(data_pos, next_pos, hashval);
//std::cout << "havalllllll  " << hashval<< " " << bucket_page->ele[offset / sizeof(hashElement)].hashval << std::endl;
                    //update log
                    ++loghdr.bkt_log_num;
                    page_object_wrapper<RawDB::bkt_pgHDR> hdr(std::move(bkt_pg.share(0, sizeof(bkt_pgHDR))));
                    logrec.bkt_log.push_back(bkt_logRecord(page_id, hdr->pg_lsn, hdr.get_it(), offset, hashElement(data_pos, next_pos, hashval)));

                    //update bkt page lsn
                    bucket_page->pgHDR.pg_lsn = logrec.lsn;

                    finish_page_for_write(pg);
                    return Pointer_offset(page_id, offset);
                });
            });
        });
    }

    /**
     * @brief insert a kv into the database, and return the inserted posision
     * 
     * @param _insertion 
     * @return future<Pointer_slot> 
     */
    future<Pointer_slot> insert_data_item(item_insertion_data& _insertion, LogHeader& loghdr, LogRecord& logrec) {
        
        return seastar::do_with(std::string(), [this, &_insertion, &loghdr, &logrec] (auto& pg) {
            return _bitmap.ReturnPage(_insertion.db_id, _insertion.tab_id, "data").then([this, &_insertion, &loghdr, &logrec, &pg] (pg_id data_page_id) {
                pg = get_data_page_name(_insertion.db_id, _insertion.tab_id, data_page_id);
                return prepare_page_for_write(pg).then([this, &_insertion, data_page_id, &pg, &loghdr, &logrec] (seastar::temporary_buffer<char> data_pg) {
                    RawDB::page_object_wrapper<RawDB::data_pgHDR> data_pg_hdr(data_pg.share(0, data_page_header_size));
                    //get the write pos and slot num
     
                    LocationIndex write_pos = get_data_write_pos(data_pg_hdr, _insertion.key.key().size() + _insertion.value.size()); 
       std::cout << "write_pos is " << write_pos << std::endl;
                    //get a slot num from page's bitmap, and update it
                    LocationIndex slot_num = get_one_slot(data_pg_hdr);

                    
                    //update_slot(data_pg_hdr, slot_num, write_pos);
                    data_pg_hdr->slot[slot_num] = write_pos;

                    //write log
                    // loghdr.item_sz = _insertion.key.key().size() + _insertion.value.size();
                    // logrec.data_page_id = data_page_id;
                    // logrec.old_data_lsn = data_pg_hdr->pg_lsn;
                    // logrec.data_pg_header = data_pg_hdr.get_it();
                    // logrec.data_offset = write_pos;
                    // logrec.item = Item(_insertion.key.key().size() ,_insertion.value.size(), _insertion.key.key().c_str() ,_insertion.value.c_str());

                    //update data page lsn
                    data_pg_hdr->pg_lsn = logrec.lsn;

                    write_data_page(data_pg.share(), write_pos, _insertion);
                    finish_page_for_write(pg);
                    return Pointer_slot(data_page_id, slot_num);
                    
                });
            });
        });
        
    }

    LocationIndex get_one_slot(page_object_wrapper<data_pgHDR>& data_pg_hdr) {
        for(int i = 0; i < 32; ++i) {
            uint8_t tmp = 1;
            do {
                if((tmp & data_pg_hdr->bitmap[i]) == 0) {
                    data_pg_hdr->bitmap[i] |= tmp;
                    return i * 32 + tmp - 1;
                }
                tmp <<= 1;
            } while(tmp != 1);
        }
        //256为非法值，此处应做异常处理
        return 256;
    }

    LocationIndex get_data_write_pos(page_object_wrapper<data_pgHDR>& data_pg_hdr, int item_len) {
        LocationIndex write_pos = data_pg_hdr->pg_lower;
        data_pg_hdr->pg_lower += item_len;
        return write_pos;
    }

    inline
    void update_slot(page_object_wrapper<data_pgHDR>& data_pg_hdr, LocationIndex& slot_num, LocationIndex& write_pos) {
        data_pg_hdr->slot[slot_num] = write_pos;
    }

    /**
     * @brief write a kv into the page at pos write_pos
     * 
     * @param data_pg the page
     * @param write_pos the position where to write kv
     * @param _insertion the item, include key and value
     */
    void write_data_page(seastar::temporary_buffer<char> data_pg, LocationIndex write_pos, item_insertion_data& _insertion) {
        page_object_wrapper<RawDB::data_page> wrapper(std::move(data_pg));
        RawDB::Item itm(_insertion.key.key().size(), _insertion.value.size(), _insertion.key.key().c_str(), _insertion.value.c_str());
        memcpy(&wrapper->data[write_pos], &itm, sizeof(itm));
    }

    future<> load_page_from_disk(std::string pg) {
        
        return seastar::do_with(std::string(), uint32_t(), std::string(), pg_id(), [this, pg] (auto& db, auto& table, auto& fi, auto& page) {
            analyze_page_name(pg, db, table, fi, page);
            //std::cout << "pg is: " << pg << std::endl << "after analyse: " <<  db << " " << table << " " << fi << " " << page << std::endl;
            return _disk.ReadPage(db, table, fi, page).then([this, pg] (seastar::temporary_buffer<char> buf) {
                //std::cout << "finish read" << std::endl;
                return insert_page_to_cache(pg, std::move(buf));
            });
        }); 
        
    }

    future<seastar::temporary_buffer<char>> prepare_page_for_read(std::string pg) {
        auto it = cache.exist_page(pg);
        //if not found the page in cache, load it from disk
        if(it == cache.not_exist_page()) {
            std::cout<<"page not found" << std::endl;
            return load_page_from_disk(pg).then([this, pg] () {
                return cache.exist_page(pg)->second.pg.share();
            });
        }
        it->second.lo.read_lock();
        return make_ready_future<seastar::temporary_buffer<char>>((it->second.pg.share()));
    }

    void finish_page_for_read(std::string pg) {
        auto it = cache.exist_page(pg);
        assert(("page not found", it != cache.not_exist_page()));
        it->second.lo.read_unlock();
    }

    future<seastar::temporary_buffer<char>> prepare_page_for_write(std::string pg) {
        auto it = cache.exist_page(pg);
        //if not found the page in cache, load it from disk
        if(it == cache.not_exist_page()) {
            std::cout<<"page not found" << std::endl;
            return load_page_from_disk(pg).then([this, pg] () {
                return cache.exist_page(pg)->second.pg.share();
            });
        }
        it->second.lo.write_lock();
        return make_ready_future<seastar::temporary_buffer<char>>((it->second.pg.share()));
    }

    void finish_page_for_write(std::string pg) {
        auto it = cache.exist_page(pg);
        assert(("page not found", it != cache.not_exist_page()));
        make_page_dirty(it->second);
        it->second.lo.write_unlock();
    }

    void make_page_dirty(pg_node& page_node) {
        page_node.is_dirty = true;
    }

    void make_page_clean(pg_node& page_node) {
        page_node.is_dirty = false;
    }

    future<> insert_page_to_cache(std::string pg, seastar::temporary_buffer<char> buf) {
        return seastar::do_with(std::move(buf), [this, pg] (auto& buf) {
            if(cache.need_eliminate()) {
                return eliminate_one_page().then([this, pg, &buf] {
                    cache.add_page(pg, std::move(buf));
                });
            }
            cache.add_page(pg, std::move(buf));
            return make_ready_future<>();
        });
        
    }

    /**
     * @brief eliminate a page from cache
     * 
     * @return future<> 
     */
    future<> eliminate_one_page() {
        auto it = cache.get_last_page_iter();
        //get write lock to ensure there is no other user using this page
        it->second.lo.write_lock();
        //if page dirty, then flush it to disk
        if(it->second.is_dirty) {
            return seastar::do_with(std::move(it), [this] (auto& it) {
                db_id db;
                tab_id tab;
                std::string fi;
                pg_id pg;
                analyze_page_name(it->first, db, tab, fi, pg);
                return flush_to_disk(db, tab, fi, pg, it->second.pg.share()).then([this, &it] {
                    cache.eliminate_page(it);
                    return make_ready_future<>();
                });
            });
            
        }
        else {
            cache.eliminate_page(it);
        }
        return make_ready_future<>();
        
    }

    Log& create_log(item_insertion_data& _insertion) {
        uint64_t lsn = _lsn++;
        seastar::promise<int> pro;
        LogRecord rec(lsn, LogRecordType::INSERT, _insertion.db_id, _insertion.tab_id, _insertion.key.key(), _insertion.value);
        LogHeader hdr;
        _log_buffer.push_back(std::move(Log(std::move(pro), hdr, rec)));
        return _log_buffer.back();
    }

    void flush_log() {
        seastar::with_semaphore(limit, 1, [=] {
            auto f = _log_buffer.front().finish.get_future();
            f.get();
            //AppendLogRecord()要负责将日志刷盘
            //log_mgr.AppendLogRecord(_log_buffer.front()._log_rec).then([=] (bool res){
            _disk.WriteLog(trans_log_to_buffer(_log_buffer.front())).then([=] {
                //assert((res == true));
                _log_buffer.front().pro.set_value();
                _log_buffer.pop_front();
            });
        });
    }

    future<> flush_to_disk(db_id db, tab_id tab, std::string fi, pg_id pg, seastar::temporary_buffer<char> buf) {
        //此处应该先检查落盘的日志lsn，如果不够新则要先刷一下日志

        return _disk.WritePage(db, tab, fi, pg, std::move(buf));
    }

    seastar::temporary_buffer<char>&& trans_log_to_buffer(Log& l) {
        //未debug！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
        seastar::temporary_buffer<char> buf(sizeof(l._log_hdr) + sizeof(l._log_rec));
        disk_log _log(l);
        *((disk_log*)buf.get_write()) = _log;
        return std::move(buf);
    }

    /**
     * @brief analyze the page_name, divide it into three parts
     * db_id, table_id and page_id
     * 
     * @param pg 
     * @param db 
     * @param table 
     * @param page 
     */
    void analyze_page_name(std::string pg, std::string& db, uint32_t& table, std::string& fi, pg_id& page) {
        std::regex re("\\s+");
        std::vector<std::string> ele(std::sregex_token_iterator(pg.begin(), pg.end(), re, -1),
            std::sregex_token_iterator());
        db = ele[0];
        table = atoi(ele[1].c_str());
        fi = ele[2];
        page = atoi(ele[3].c_str());
    }
    
    /**
     * @brief insert a new bucket to the table and move the old
     * bucket's element into new bucket
     * but should check whether need to alloc a new segment
     * 
     * @return true if expand successfully
     * @return false if not
     */
    future<> expand_table(db_id db, tab_id tab, RawDB::hashTAB& htb, LogHeader& loghdr, LogRecord& logrec) {
        return make_ready_future<>();
        //未debug！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
        if(!htb.need_more_bucket())
            return make_ready_future<>();
        RawDB::Metadata& meta = htb.get_metadata();
        bkt_id new_bkt_id = meta.max_bucket + 1;
        seg_id new_seg_id = new_bkt_id >> meta.seg_shift;
        //若新的segment_id大于等于目前的segment数量，则需再分配一个segment
        /*create_seg要完成创建一个segment文件，形参是一个bool，如果bool为false则代表不需要创建，直接返回即可,
        即//若新的segment_id大于等于目前的seg文件可容纳的最大segment数，则再创建一个新的seg文件并且初始化好所有的page？*/
        //return _disk.create_seg(new_seg_id >= meta.seg_num).then([this, db, tab, &htb, new_bkt_id, new_seg_id] () {!!!!!!!!!!!!!!!!!!!
        return _disk.CreateDB("").then([this, db, tab, &htb, new_bkt_id, new_seg_id, &loghdr, &logrec] () {
            RawDB::Metadata& meta = htb.get_metadata();
            ++meta.seg_num;

            //创建了一个新的bucket
            ++meta.max_bucket;

            //得到对应新桶的旧桶，将来要把旧桶中的元素重新分配到新桶中
            bkt_id old_bkt_id = new_bkt_id & meta.low_mask;

            //若新桶id达到了新的2的n次幂，则更新低位掩码与高位掩码，分配左移一位，并补1
            if(new_bkt_id > meta.high_mask) {
                meta.low_mask = meta.high_mask;
                meta.high_mask = new_bkt_id | meta.low_mask;
            }

            //update log metadata
       //     logrec.meta = meta;记得取消注释！！！！！！！！！！！！！！！！！！！

            return seastar::do_with(Pointer_offset(), Pointer_offset(), Pointer_offset(), bool(), [ =, &htb, &loghdr, &logrec] (auto& cur_pos, auto& next_pos, auto& first_pos, auto& stop_loop) {
                RawDB::Metadata& meta = htb.get_metadata();
                return get_first_ele_pos(db, tab, old_bkt_id).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] (Pointer_offset first) {
                    first_pos = first;
                    cur_pos = first;
                    stop_loop = false;
                    //if the bkt is empty, return
                    if(cur_pos.offset == 0)
                        return make_ready_future<>();
                    return get_next_ele_pos(db, tab, cur_pos).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] (Pointer_offset next) {
                        next_pos = next;
                    }).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] () {
                        //find the first ele which is not demand move, it is cur_pos
                        return seastar::do_until([&stop_loop, &cur_pos] { return stop_loop || cur_pos.offset == 0; }, [ =, &meta, &cur_pos, &next_pos, &stop_loop, &loghdr, &logrec] {
                            return get_ele_hashval(db, tab, cur_pos).then([ =, &meta, &cur_pos, &next_pos, &stop_loop, &loghdr, &logrec] (uint32_t hashval) {
                                //if find, then stop loop
                                if(((hashval & meta.high_mask) == meta.max_bucket) || next_pos.offset == 0) {
                                    stop_loop = true;
                                    //broken the chain
                                    return update_ele_next_pos(db, tab, cur_pos, Pointer_offset{0, 0}, loghdr, logrec);
                                }
                                cur_pos = next_pos;
                                return get_next_ele_pos(db, tab, cur_pos).then([this, &next_pos] (Pointer_offset next) {
                                    next_pos = next;
                                });
                            });
                        }).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] {
                            //update old bkt's first ele pos
                            return update_first_ele_pos(db, tab, old_bkt_id, next_pos, loghdr, logrec).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] {
                                //update new bkt's first ele pos
                                return update_first_ele_pos(db, tab, new_bkt_id, first_pos, loghdr, logrec).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] {
                                    //if old bkt is empty, return
                                    if(next_pos.offset == 0)
                                        return make_ready_future<>();
                                    return get_next_ele_pos(db, tab, cur_pos).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] (Pointer_offset next) {
                                        cur_pos = next_pos;
                                        next_pos = next;
                                        //next_pos must be not empty,or it will return from do_until
                                        stop_loop = (next_pos.offset == 0 ? true : false);
                                        //loop until achieve the end of old bkt chain
                                        return seastar::do_until([&stop_loop] { return stop_loop; }, [ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] {
                                            return get_ele_hashval(db, tab, next_pos).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] (uint32_t hash) {
                                                //if need to move next_pos to new bkt
                                                if(((hash & meta.high_mask) == meta.max_bucket)) {
                                                    return get_next_ele_pos(db, tab, next_pos).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] (Pointer_offset next) {
                                                        //update cur_pos's next to next
                                                        return update_ele_next_pos(db, tab, cur_pos, next, loghdr, logrec).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] {
                                                            //update next_pos's next to first_pos
                                                            return update_ele_next_pos(db, tab, next_pos, first_pos, loghdr, logrec).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop, &loghdr, &logrec] {
                                                                first_pos = next_pos;
                                                                next_pos = next;
                                                                if(next_pos.offset == 0)
                                                                    stop_loop = true;
                                                            });
                                                        });
                                                    });
                                                }
                                                return get_next_ele_pos(db, tab, next_pos).then([ =, &cur_pos, &next_pos, &first_pos, &meta, &stop_loop] (Pointer_offset next) {
                                                    cur_pos = next_pos;
                                                    next_pos = next;
                                                    if(next_pos.offset == 0)
                                                        stop_loop = true;
                                                });
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    };

    inline
    bkt_id get_bkt_id(uint32_t _hash, hashTAB& hdr) {
        return _hash & hdr.get_metadata().low_mask;
    }

    inline
    std::string get_seg_page_name(const std::string& db_id
                            , const tab_id table_id
                            , const pg_id page_id) {
        return std::string(db_id + " " + std::to_string(table_id) + " seg " + std::to_string(page_id));
    }

    inline
    std::string get_bkt_page_name(const std::string& db_id
                            , const tab_id table_id
                            , const pg_id page_id) {
        return std::string(db_id + " " + std::to_string(table_id) + " bkt " + std::to_string(page_id));
    }

    inline
    std::string get_data_page_name(const std::string& db_id
                            , const tab_id table_id
                            , const pg_id page_id) {
        return std::string(db_id + " " + std::to_string(table_id) + " dat " + std::to_string(page_id));
    }
    

};

}
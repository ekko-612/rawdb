#pragma once

#include <unordered_map>
#include <string>
#include <list>
#include <iostream>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/sstring.hh>
#include <include/backend/type_define.h>
#include <include/manager/disk_manager.h>
#include <include/stroage/seg_page.h>
#include <include/stroage/bkt_page.h>
#include <include/stroage/data_page.h>

#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>

using namespace seastar;

namespace RawDB {

class lru_node;

class pg_node {
public:
    bool is_dirty;
    std::list<lru_node>::iterator lru_iter;
    seastar::rwlock lo;
    seastar::temporary_buffer<char> pg;
    
public:
    pg_node() : is_dirty(false), pg(8192) {};

    pg_node(seastar::temporary_buffer<char> buf) : is_dirty(false), pg(std::move(buf)) {}

    // pg_node(const pg_node& rbs) : is_dirty(rbs.is_dirty) {
    //     //pg = rbs.pg.share();
    //     std::cout << "nonono" << std::endl;
    // }

    pg_node(pg_node&& rbs) noexcept : lo(std::move(rbs.lo)), pg(std::move(rbs.pg)) {
        is_dirty = rbs.is_dirty;
        lru_iter = rbs.lru_iter;
    }

    ~pg_node() {

    }
    
};

// lru链表的结点
class lru_node {

public:

    //map的结点的指针，该指针指回对应的pg_struct
    std::unordered_map<std::string, pg_node>::iterator map_iter;   

public: 

    lru_node(std::unordered_map<std::string, pg_node>::iterator it)
        : map_iter(it) 
    {}

private:

};

/* cache
 *
 * _htb is cache, it contains many pages, which will be linked by lru_node
 * its key consists of "db_id+table_id+seg/bkt/date+page_id"
 * its value contains a rwlock for write(flush dirty and update), and a temporaray_buffer
 * 
 * every function below need to update _lru_list, it means put every page it visited
 * to the list's head
 * 
 */
class sys_cache {
public:
    
private:
    std::list<lru_node> _lru_list; 
    std::unordered_map<std::string, pg_node> _cache;
    const uint64_t cache_page_max_num = 786432;//6GB
    
public:
    sys_cache() = default;

    std::unordered_map<std::string, RawDB::pg_node>::iterator exist_page(std::string pg) {
        auto it = _cache.find(pg);
        if(it != _cache.end()) {
            put_this_lru_node_to_head(it);
        }
        return it;
    }

    std::unordered_map<std::string, RawDB::pg_node>::iterator not_exist_page() {
        return _cache.end();
    }

//use for test
    void print_page() {
        for(const auto& it : _cache) {
            std::cout << it.first << std::endl;
        }
    }

//use for test
    void add_page() {
        //for test
        pg_node sg_pg;
        seg_page seg;
        bkt_page bkt;
        seg.bkt[0] = Pointer_offset(0, sizeof(bkt.pgHDR));
        //seg.bkt[1] = Pointer_offset(0, sizeof(bkt.pgHDR)+sizeof(Pointer_offset));
        *((seg_page*)(sg_pg.pg.get_write())) = seg;


seastar::temporary_buffer<char> buf(8192);
*((seg_page*)(buf.get_write())) = seg;
        add_page("0 0 seg 0", std::move(buf));
seastar::temporary_buffer<char> buf2(8192);
        add_page("0 0 seg 1", std::move(buf2));
        //put_this_lru_node_to_head(_cache.find("0 0 seg 0"));
        

       // _cache.insert({"0 0 seg 0", std::move(sg_pg)});
        

        hashElement ele0 {{0, 0}, {0, sizeof(bkt.pgHDR)+sizeof(hashElement)}, std::hash<std::string>()("1"), 0, 0 };
        hashElement ele1 {{0, 1}, {0, sizeof(bkt.pgHDR)+2*sizeof(hashElement)}, std::hash<std::string>()("2"), 0, 0 };
        hashElement ele2 {{0, 2}, {0, 0}, std::hash<std::string>()("3"), 0, 0 };

        bkt.ele[0] = ele0;
        bkt.ele[1] = ele1;
        bkt.ele[2] = ele2;

        // Pointer_slot item_pos;
        // Pointer_offset next;
        // uint32_t hashval;    //key经过hash函数计算得到的hashvalue
        // int flag;
        // int encoding;    //value编码方式
        // char padding[20];
        pg_node bk_pg;
        *((bkt_page*)(bk_pg.pg.get_write())) = bkt;
        add_page("0 0 bkt 0", std::move(bk_pg.pg));

         pg_node dt_pg;
         data_page data;
         data.pgHDR.slot[0] = sizeof(data_pgHDR);
         std::string key = "hello!!";
         std::string val = "world!!";
         Item item(sizeof(key), sizeof(val), key.c_str(), val.c_str());
         memcpy(data.data, &item, sizeof(item)); 
         *((data_page*)(dt_pg.pg.get_write())) = data;
        add_page("0 0 data 0", std::move(dt_pg.pg));

    
    }

    void add_page(std::string pg, seastar::temporary_buffer<char> buf) {
        pg_node node(std::move(buf));
        //insert a page into cache and a lru_node into lru_list
        auto it = _cache.insert({pg, std::move(node)});
        if(it.second) {
            _lru_list.push_front(it.first);
            it.first->second.lru_iter = _lru_list.begin();
        } 
        //若到此处则表示插入失败，应该抛出异常
    }

    bool need_eliminate() {
        return _cache.size() >= cache_page_max_num;
    }

    std::unordered_map<std::string, RawDB::pg_node>::iterator get_last_page_iter() {
        return _lru_list.back().map_iter;
    }

    void eliminate_page(std::unordered_map<std::string, RawDB::pg_node>::iterator it) {
        _lru_list.erase(it->second.lru_iter);
        _cache.erase(it);
    }

    
private:

    void put_this_lru_node_to_head(std::unordered_map<std::string, RawDB::pg_node>::iterator it) {
        _lru_list.push_front(it);
        _lru_list.erase(it->second.lru_iter);
        it->second.lru_iter = _lru_list.begin();
    }

    
}; 

class dirty_list {
public: 
    
};

class item_key {
private:
    std::string _key;
    uint32_t _hash;
public:
    item_key() = default;

    item_key(const item_key& rbs) : _key(rbs._key) , _hash(rbs._hash) 
    {}

    item_key(std::string key): _key(key), _hash(std::hash<std::string>()(key))
    {};
    
    item_key(item_key&& other): _key(std::move(other._key)) , _hash(other._hash) {
        //other._hash = 0;
    }

    uint32_t hash() const {
        return _hash;
    }
    
    const std::string& key() const {
        return _key;
    }
    
    bool operator==(const item_key& other) const {
        return other._hash == _hash && other._key == _key;
    }
    
    void operator=(item_key&& other) {
        _key = std::move(other._key);
        _hash = other._hash;
       // other._hash = 0;
    }
    
    void operator=(std::string&& other) {
        _key = std::move(other);
        _hash = std::hash<std::string>()(other);
    }
};

struct item_insertion_data {
    std::string db_id;
    uint32_t tab_id;
    item_key key;
    std::string value;

    item_insertion_data() = default;

    item_insertion_data(std::string db_id, uint32_t tab_id, item_key key, std::string value)
        : db_id(db_id)
        , tab_id(tab_id)
        , key(key)
        , value(value)
    {}

    item_insertion_data(const item_insertion_data& rbs) 
        : db_id(rbs.db_id)
        , tab_id(rbs.tab_id)
        , key(key)
        , value(rbs.value) 
    {}

    item_insertion_data(item_insertion_data&& rbs)
        : db_id(std::move(rbs.db_id))
        , tab_id(rbs.tab_id)
        , key(std::move(rbs.key))
        , value(std::move(rbs.value)) {
        rbs.tab_id = 0;
    }
};


}
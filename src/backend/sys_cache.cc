#include <include/backend/sys_cache.h>
#include <include/stroage/data_page.h>
#include <include/backend/kv_server.h>

// //通过share方法返回一个page
// temporary_buffer<char> RawDB::sys_cache::get_page_for_read(std::string pg) {
//     if(_cache.find(pg)) {
//         put_this_node_to_head();
//         return 
//     }
//     //_lru_list.push_front(lru_node(_htb.insert(std::make_pair(page_id, seastar::temporary_buffer<char>(_buf, 8192))).first));

// }

// RawDB::Pointer_slot RawDB::sys_cache::write_data_page(item_insertion_data& _insertion) {
//     //get a approprite data page id from fsm to insert this key-value
//     page_id data_pg_id = fsm(_insertion.value.size());

//     temporary_buffer<char> data_pg = get_page(
//         _insertion.db_id + std::to_string(_insertion.tab_id) + "data" + std::to_string(data_pg_id));
//     RawDB::page_object_wrapper<RawDB::data_pgHDR> data_pg_hdr(std::move(data_pg.share(
//         0, data_page_header_size)));
//     LocationIndex write_pos = data_pg_hdr->pg_lower;
// }

#include <include/manager/disk_manager.h>
#include <include/manager/log_stream.h>
#include <include/stroage/bkt_page.h>
#include <include/stroage/seg_page.h>
#include <include/stroage/data_page.h>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/app-template.hh>

namespace RawDB {

// DiskManager::DiskManager() {
//     aligned_buf = temporary_buffer<char>::aligned(DiskManager::DMA_ALIGNMENTSIZE, 4096);
//     logfile_head = temporary_buffer<char>::aligned(DiskManager::DMA_ALIGNMENTSIZE, 4096);
//     logfile_open = false;
    
// }

future<> DiskManager::Init() {
    return create_dir(storage_dir_name() + "/data").then([this](){
        return create_dir(get_log_dir());
    }).then([this](){
        return get_checkpoint_lsn().then([this](lsn_t lsn){
            checkpoint_lsn = lsn;
            std::string log_name = gen_logname_and_offset(checkpoint_lsn).first;
            cur_log_index = get_log_index(log_name);
            
            return file_exists(log_name).then([this, log_name](bool exist) {
                if (!exist) {
                    return open_file_dma(log_name, open_flags::rw|open_flags::create|open_flags::truncate).then([this](file f){
                        strcpy(logfile_head.get_write(), std::to_string(LOGFILEHEADSIZE).c_str());
                        return WriteData(logfile_head.share(), f, 0, logfile_head.size()).then([this](){
                            // cur_log_len = LOGFILEHEADSIZE;
                            // buf_offset = 0;
                        }).finally([f]()mutable{
                            return f.close().finally([f]{});
                        });
                    });
                } else {
                    return make_ready_future<>();
                }
            }).then([this](){
                return repeat([this](){
                    std::string nxt_logfile = gen_logname(cur_log_index + 1);
                    return file_exists(nxt_logfile).then([this](bool exist){
                        if (exist) {
                            cur_log_index++;
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        } else {
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                    });
                }).then([this](){
                    std::string log_name = gen_logname(cur_log_index);
                    return open_file_dma(log_name, open_flags::rw).then([this](file f){
                        log_file = f;
                        logfile_open = true;
                        return GetLogFileLen().then([this](uint32_t len){
                            cur_log_len = len;
                            if (cur_log_len % DMA_ALIGNMENTSIZE == 0) {
                                buf_offset = 0;
                                return make_ready_future<>();
                            } else {
                                return ReadData(log_file, cur_log_len - (cur_log_len % DMA_ALIGNMENTSIZE), aligned_buf.size(), aligned_buf.share()).then([this](){
                                    buf_offset = (cur_log_len % DMA_ALIGNMENTSIZE);
                                });
                                
                            }
                        });
                    });
                });
            });    
        });
    });
    
}

DiskManager::~DiskManager() {
    if (logfile_open) {
        log_file.close().get0();
    }
}

// data必须由调用方do_with保护
// 并且data必须对齐，len也对齐
future<> DiskManager::WriteData(temporary_buffer<char> data, file f, uint32_t pos, uint32_t len) {
    assert(pos % DiskManager::DMA_ALIGNMENTSIZE == 0);
    assert(len % DiskManager::DMA_ALIGNMENTSIZE == 0);
    assert(data.size() >= len);
    if (len <= 0) return make_ready_future<>();
    
    return do_with(uint32_t{0}, std::move(pos), std::move(len), [f, &data](auto& write_size, auto& pos, auto& len)mutable{
        return repeat([f, &write_size, &pos, &len, &data]()mutable{
            if (write_size >= len) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            return f.dma_write(pos, data.get() + write_size, len - write_size).then([f, &write_size, &pos, &len](size_t cur_write_size)mutable{
                if (cur_write_size % DiskManager::DMA_ALIGNMENTSIZE == 0) {
                    write_size += cur_write_size;
                    pos += cur_write_size;
                }
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
        });
    }).then([f]()mutable{
        return f.flush().finally([f]{});
    });
}

// buf必须对齐，且已经由do_with保护
// pos、aligned_len也要对齐
future<> DiskManager::ReadData(file f, uint32_t pos, uint32_t aligned_len, temporary_buffer<char> buf) {
    assert(pos % DiskManager::DMA_ALIGNMENTSIZE == 0);
    assert(aligned_len % DiskManager::DMA_ALIGNMENTSIZE == 0);
    assert(buf.size() >= aligned_len);
    return do_with(uint32_t{0}, std::move(pos), std::move(aligned_len), [f, &buf](auto& read_size, auto& pos, auto& len)mutable{
        return repeat([f, &read_size, &pos, &len, &buf]()mutable{
            if (read_size >= len) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            return f.dma_read(pos, buf.get_write() + read_size, len - read_size).then([&read_size, &pos](size_t cur_read_size){
                if (cur_read_size % DiskManager::DMA_ALIGNMENTSIZE == 0) {
                    read_size += cur_read_size;
                    pos += cur_read_size;
                }
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
        });
    });
}


// CRC的判断放在buffer pool
// page_data需要对齐
seastar::future<> DiskManager::WritePage(std::string db, uint32_t table_id, std::string fi, pg_id page_id, seastar::temporary_buffer<char> page_data) {
    assert(page_data.size() == DiskManager::PAGESIZE);
    assert(page_data.size() % DMA_ALIGNMENTSIZE == 0);
    auto pir = gen_filename_and_offset(db, table_id, fi, page_id);
    std::string file_name = pir.first;
    uint32_t offset = pir.second;
    assert(offset % DMA_ALIGNMENTSIZE == 0);
    std::cout << "open file : " << file_name <<" success! " << std::endl;
    return do_with(std::move(page_data), std::move(file_name), [this, offset](auto& tembuf, auto& filename) {
        return open_file_dma(filename, open_flags::wo).then([this, offset, &tembuf](file f){
            return WriteData(tembuf.share(), f, offset, tembuf.size()).finally([f]()mutable {
                return f.close().finally([f]{});
            });
        });
        
    });
}

seastar::future<seastar::temporary_buffer<char>> DiskManager::ReadPage(std::string db, uint32_t table_id, std::string fi, pg_id page_id) {
    auto pir = gen_filename_and_offset(db, table_id, fi, page_id);
    std::string file_name = pir.first;
    uint32_t offset = pir.second;
    assert(offset % DiskManager::DMA_ALIGNMENTSIZE == 0);
    std::cout << "open file : " << file_name <<" success! " << std::endl;
    return open_file_dma(file_name, open_flags::ro | open_flags::create).then([this, offset](file f){
        auto buf = temporary_buffer<char>::aligned(DiskManager::DMA_ALIGNMENTSIZE, DiskManager::PAGESIZE);
        return do_with(std::move(buf), [this, offset, f = std::move(f)](auto& tembuf){
            return ReadData(f, offset, tembuf.size(), tembuf.share()).then([&tembuf](){
                return tembuf.share();
            }).finally([f = std::move(f)]()mutable{
                return f.close().finally([f = std::move(f)]{});
            });
            
        });
    });
}


future<> DiskManager::CheckIfNewLogFile() {
    if (cur_log_len >= LOGFILESIZE) {
        assert(buf_offset == 0);
        return log_file.close().then([this](){
            logfile_open = false;
            cur_log_index++;
            std::string logname = gen_logname(cur_log_index);
            return open_file_dma(logname, open_flags::create|open_flags::rw|open_flags::truncate).then([this](file f){
                strcpy(logfile_head.get_write(), std::to_string(LOGFILEHEADSIZE).c_str());
                return WriteData(logfile_head.share(), f, 0, logfile_head.size()).then([this](){
                    cur_log_len = LOGFILEHEADSIZE;
                }).finally([f, this](){
                    log_file = f;
                    logfile_open = true;
                });
            });
        });
    }
    return make_ready_future<>();
}

// 更新当前日志文件的长度
future<> DiskManager::UpdateLogFileLen(uint32_t file_length) {
    strcpy(logfile_head.get_write(), std::to_string(file_length).c_str());
    return WriteData(logfile_head.share(), log_file, 0, logfile_head.size());
}
// 获得当前日志文件的长度
future<uint32_t> DiskManager::GetLogFileLen() {

    return ReadData(log_file, 0, logfile_head.size(), logfile_head.share()).then([this](){
        return make_ready_future<uint32_t>(atoi(logfile_head.get()));
    });
}

future<uint32_t> DiskManager::GetLogFileLen(file f) {
    auto buf = temporary_buffer<char>::aligned(DiskManager::DMA_ALIGNMENTSIZE, LOGFILEHEADSIZE);
    return do_with(std::move(buf), f = std::move(f), [&](auto& buf, auto& f){
        return repeat([&f, &buf]()mutable{
            return f.dma_read(0, buf.get_write(), LOGFILEHEADSIZE).then([f, &buf](size_t read_size)mutable{
                if (read_size >= LOGFILEHEADSIZE) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
        }).then([f, &buf](){
            return make_ready_future<uint32_t>(atoi(buf.get()));
        });
    });
    
}

// 读取日志
// seastar::future<std::shared_ptr<LogStream>> DiskManager::ReadLog(uint64_t start_lsn) {
//     std::pair<std::string, uint32_t> pir = DiskManager::gen_logname_and_offset(start_lsn);
//     std::string logname = pir.first;
//     uint32_t offset = pir.second;
//     uint32_t index = get_log_index(logname);
//     auto logstream_ptr = std::make_shared<LogStream>(logname, offset, index);
//     return make_ready_future<std::shared_ptr<LogStream>>(std::move(logstream_ptr));
// }

seastar::future<> DiskManager::WriteLog(temporary_buffer<char> log_data) {
    return do_with(std::move(log_data), [this] (auto& log_data){
        return CheckIfNewLogFile().then([this, &log_data](){
        
            
            if (cur_log_len + log_data.size() > DiskManager::LOGFILESIZE) {
                uint32_t firlen = DiskManager::LOGFILESIZE - cur_log_len;
                uint32_t seclen = log_data.size() - firlen;
                
                return WriteLog(log_data.share(0, firlen)).then([firlen, seclen, &log_data, this](){
                    return WriteLog(log_data.share(firlen, seclen));
                });
            }
            assert(buf_offset >= 0 && buf_offset < aligned_buf.size());
            if (buf_offset == 0) {
                // 此时log_data是对齐的
                uint32_t aligned_len = log_data.size() - (log_data.size() % DMA_ALIGNMENTSIZE);
                
                return WriteData(log_data.share(), log_file, cur_log_len, aligned_len).then([aligned_len, this, &log_data](){
                    cur_log_len += aligned_len;
                    if (aligned_len < log_data.size()) {
                        memcpy(aligned_buf.get_write(), log_data.get() + aligned_len, log_data.size() - aligned_len);
                        buf_offset = log_data.size() - aligned_len;
                        return WriteData(aligned_buf.share(), log_file, cur_log_len, aligned_buf.size()).then([this](){
                            cur_log_len += buf_offset;
                            return UpdateLogFileLen(cur_log_len);
                        });
                    }
                });
            } 
            uint32_t pre_len = aligned_buf.size() - buf_offset;
            if (log_data.size() > pre_len) {
                memcpy(aligned_buf.get_write() + buf_offset, log_data.get(), pre_len);
                cur_log_len -= buf_offset;
                return WriteData(aligned_buf.share(), log_file, cur_log_len, aligned_buf.size()).then([this, pre_len, &log_data](){
                    cur_log_len += aligned_buf.size();
                    buf_offset = 0;
                    return WriteLog(log_data.share(pre_len, log_data.size() - pre_len));
                });
            } else {
                memcpy(aligned_buf.get_write() + buf_offset, log_data.get(), log_data.size());
                cur_log_len -= buf_offset;
                buf_offset += log_data.size();
                return WriteData(aligned_buf.share(), log_file, cur_log_len, aligned_buf.size()).then([this, &log_data](){
                    cur_log_len += buf_offset;
                    return UpdateLogFileLen(cur_log_len);
                });
            }
        });
    });


}



future<lsn_t> DiskManager::get_checkpoint_lsn() {
    std::string filename(checkpoint_begin_lsn_file_name);
    return file_exists(filename).then([filename = std::move(filename)](bool exist)mutable {
        if (exist) {
            return open_file_dma(filename, open_flags::ro).then([](file f){
                return do_with(make_file_input_stream(f), [](auto& input){
                    return input.read().then([&input](temporary_buffer<char> tembuf)mutable{
                        lsn_t res = (lsn_t)atoi(tembuf.get());
                        return input.close().then([res](){
                            return make_ready_future<lsn_t>(res);
                        });
                    });
                }).finally([f]()mutable{
                    return f.close().finally([f]{});
                });
            });
        } else {
            return open_file_dma(filename, open_flags::create | open_flags::wo).then([](file f){
                return do_with(make_file_output_stream(f, 4096), [](auto& output) {
                    return output.write("0").then([&output](){
                        return output.flush().then([](){
                            return make_ready_future<lsn_t>(0);
                        });
                    });
                }).finally([f]()mutable{
                    return f.close().finally([f]{});
                });
            });
        }
    });
}

future<lsn_t> DiskManager::GetCheckPointBeginLSN() {
    return make_ready_future<lsn_t>(checkpoint_lsn);
}

future<> DiskManager::SetCheckPointBeginLSN(lsn_t lsn) {
    std::string filename(checkpoint_begin_lsn_file_name);
    return open_file_dma(filename, open_flags::create | open_flags::wo).then([lsn](file f) {
        return do_with(make_file_output_stream(f, 4096), [lsn](auto& output){
            return output.write(std::to_string(lsn)).then([&output]()mutable{
                return output.flush();
            });
        }).finally([f]()mutable{
            return f.close().finally([f]{});
        });
        
    });
}

//=========================================================================================================
// 以下皆是文件操作

/* 首先是对log文件的命名，log文件命名方式为：
BASE_DIR/core1/log/1
BASE_DIR/core1/log/2
BASE_DIR/core1/log/3
...
BASE_DIR由自己设置
*/

// 存储page或者log的起始文件夹名字，即BASE_DIR
std::string DiskManager::storage_dir_name() {
    return "/root/workspace/rawdb";
}

// 返回存储log文件的文件夹
std::string DiskManager::get_log_dir() {
    return storage_dir_name() + "/log/" +  std::to_string(this_shard_id()) ;
}

// 返回第log_index个日志的文件名
std::string DiskManager::gen_logname(uint32_t log_index) {
    return get_log_dir() + "/" + std::to_string(log_index);
}

// 得到日志文件名和偏移量，注意返回的offset已经包含了logfile的文件头长度
std::pair<std::string, uint32_t> DiskManager::gen_logname_and_offset(uint64_t pos) {
    uint32_t realLogLen = LOGFILESIZE - LOGFILEHEADSIZE;
    uint32_t log_index = pos / realLogLen;
    uint32_t offset = pos % realLogLen;
    return {gen_logname(log_index), offset + LOGFILEHEADSIZE};
}

// 返回当前是第几个日志文件
uint32_t DiskManager::get_log_index(std::string logname) {
    size_t idx = logname.find_last_of('/');
    return stoi(logname.substr(idx + 1));
}


/* 然后是对table文件的命名，table文件命名方式尚未搞清楚，因为不知道table_id和fi是什么
BASE_DIR/core1/table/db_name/？？？？ 后面是table_id？搞不太清楚结构
BASE_DIR/core1/table/db_name/？？？
BASE_DIR/core1/table/db_name/？？？
...
BASE_DIR同样由自己设置
*/


std::string DiskManager::get_table_dir(std::string db, uint32_t table_id) {
    return std::string(storage_dir_name() + "/data/" + "db_" + db + "/table_" + std::to_string(table_id));
}

// 得到文件名和偏移量
std::pair<std::string, uint32_t> DiskManager::gen_filename_and_offset(std::string db, uint32_t table_id, std::string fi, pg_id page_id) {
    //每个文件的前2页为bitmap页
    std::string file_name(get_table_dir(db, table_id) + "/" + std::to_string(page_id / file_size[fi]) + "." + fi);
    return {file_name, (page_id % file_size[fi]) * 8192};
    // table_id是什么？
    // 不知道怎么起名字
}


// 返回db的目录名
// 如 /core1/table/db1
std::string DiskManager::gen_db_dirname(std::string db) {
    
    return storage_dir_name() + "/data/" + "db_" + db;
    
}




// 创建一个文件夹，如果已经存在，那么什么都不会做
future<> DiskManager::create_dir(std::string dirname) {
    return recursive_touch_directory(dirname);
}
// 删除一个文件夹，如果不存在，那么会出错
future<> DiskManager::remove_dir(std::string dirname) {
    return recursive_remove_directory(dirname);
}

// 创建一个db
future<> DiskManager::CreateDB(std::string db) {
    std::string dbname = gen_db_dirname(db);
    return create_dir(dbname).then([&, dbname, db = std::move(db)] {
        return seastar::do_with(int(1),temporary_buffer<char>::aligned(4096, 8192),temporary_buffer<char>::aligned(4096, 8192),temporary_buffer<char>::aligned(4096, 8192),temporary_buffer<char>::aligned(4096, 8192), std::move(db), std::move(dbname), [&] (auto& cur_num, auto& seg_buf, auto& bkt_buf, auto& dat_buf, auto& meta_buf, auto& db, auto& dbname) {//std::cout << "?" << std::endl; 
            Metadata meta;
            strcpy(meta_buf.get_write(), (char*)&meta);
            
            RawDB::bkt_page bkt_pg;
            RawDB::seg_page seg_pg;
            RawDB::data_page dat_pg;

            strcpy(seg_buf.get_write(), (char*)&seg_pg);
            strcpy(dat_buf.get_write(), (char*)&dat_pg);
            strcpy(bkt_buf.get_write(), (char*)&bkt_pg); 
            return seastar::do_until([&cur_num] { return cur_num > 100; }, [&] {
                return create_dir(dbname + "/table_" + std::to_string(cur_num)).then([&] {
                    return open_file_dma(dbname + "/table_" + std::to_string(cur_num) + "/0.seg", open_flags::create).then([&](file f1){
                        return f1.close().then([&] {
                            return open_file_dma(dbname + "/table_" + std::to_string(cur_num) + "/0.bkt", open_flags::create).then([&] (file f2) {
                                return f2.close().then([&] {
                                    return open_file_dma(dbname + "/table_" + std::to_string(cur_num) + "/0.dat", open_flags::create).then([&] (file f3) {
                                        return f3.close().then([&] {
                                            return open_file_dma(dbname + "/table_" + std::to_string(cur_num) + "/0.meta", open_flags::create).then([&] (file f4) {
                                                return f4.close().finally([f1,f2,f3,f4] {});
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    }).then([=, &cur_num] {
                        ++cur_num;
                    });
                });
            }).then([&] {
                return do_with(int(1), uint32_t(0), [&] (auto& i, auto& pg_id) {
                    return do_until([&i] {return i > 100;}, [&] {
                        ++i;
                        pg_id = 0;
                        return WritePage(db, i - 1, "meta", 0, std::move(meta_buf.share())).then([&] {
                            return do_until([&] {return pg_id == 128;}, [&] {
                                return WritePage(db, i - 1, "seg", pg_id, std::move(seg_buf.share())).then([&] {
                                    return WritePage(db, i - 1, "bkt", pg_id, std::move(bkt_buf.share())).then([&] {
                                        return WritePage(db, i - 1, "dat", pg_id, std::move(dat_buf.share())).then([&] {
                                            ++pg_id;
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
}

// 删除一个db
future<> DiskManager::DeleteDB(std::string db) {
    std::string dbname = gen_db_dirname(db);
    return remove_dir(db);
}
}
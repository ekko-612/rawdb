#pragma once

#include <string>
#include <include/backend/type_define.h>
#include <seastar/core/future.hh>
#include <seastar/core/file.hh>
#include <seastar/util/file.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/iostream.hh>
#include <memory>
#include <include/manager/log.h>

using namespace seastar;
class LogStream;
namespace RawDB {
class DiskManager {
public:

    std::unordered_map<std::string, int> file_size{
        {"seg", 131072},
        {"bkt", 131072},
        {"dat", 262144},
        {"meta", 1}
    };

    // 写log
    file log_file; 
    uint32_t cur_log_len; // 当前logfile的长度
    uint32_t cur_log_index; // 当前属于第几个logfile
    bool logfile_open; // logfile是否已经打开
    temporary_buffer<char> aligned_buf; // 4kB且对齐
    temporary_buffer<char> logfile_head; // 4kB且对齐
    uint32_t buf_offset;

    // 判断是否需要开启新的日志文件（即判断cur_log_len是否超过限定值）
    future<> CheckIfNewLogFile();
    // 更新当前日志文件的长度
    future<> UpdateLogFileLen(uint32_t file_length);
    // 获得当前日志文件的长度
    future<uint32_t> GetLogFileLen();
    // 读log
    
    // checkpoint
    lsn_t checkpoint_lsn;
    future<lsn_t> get_checkpoint_lsn();
    
    

    //page
    future<> WriteData(temporary_buffer<char> data, file f, uint32_t pos, uint32_t len);
    future<> ReadData(file f, uint32_t pos, uint32_t len, temporary_buffer<char> buf);

    

    // 删除一个文件夹，如果不存在，那么会出错
    future<> remove_dir(std::string dirname); 
    // 存储page或者log的起始文件夹名字
    std::string storage_dir_name();
    std::string get_log_dir();
    std::string get_table_dir(std::string, uint32_t);
    std::pair<std::string, uint32_t> gen_logname_and_offset(uint64_t pos);
    std::pair<std::string, uint32_t> gen_filename_and_offset(std::string db, uint32_t table_id, std::string fi, pg_id page_id);
    std::string gen_db_dirname(std::string db);
    uint32_t get_log_index(std::string logname);

public:
    static constexpr uint32_t PAGESIZE = 8192; // page大小为8KB
    static constexpr uint64_t LOGFILESIZE = 512LL * 1024 * 1024; // log文件大小为512MB，注意包括文件头
    static constexpr uint64_t FILESIZE = 1024LL * 1024 * 1024; // 文件大小为1GB
    static constexpr uint32_t DMA_ALIGNMENTSIZE = 4096; // 对齐字节为4096
    static constexpr uint32_t LOGFILEHEADSIZE = 4096; // 日志文件头
    static constexpr char* checkpoint_begin_lsn_file_name = "checkpoint_begin_lsn"; // 专门存储checkpoint_begin_lsn的文件
    
    DiskManager() {
    aligned_buf = temporary_buffer<char>::aligned(DiskManager::DMA_ALIGNMENTSIZE, 4096);
    logfile_head = temporary_buffer<char>::aligned(DiskManager::DMA_ALIGNMENTSIZE, 4096);
    logfile_open = false;
    
    }
    ~DiskManager();

    // 初始化
    future<> Init();
    
    // 创建一个文件夹，如果已经存在，那么什么都不会做
    future<> create_dir(std::string dirname);
    
    // 创建db的目录
    future<> CreateDB(std::string db);
    // 删除对应db的目录
    future<> DeleteDB(std::string db);
    
    std::string gen_logname(uint32_t log_index);
    future<uint32_t> GetLogFileLen(file f);


    // CRC的判断放在buffer pool
    // page_data需要对齐
    // 作用：将page写到磁盘
    seastar::future<> WritePage(std::string db, uint32_t table_id, std::string fi, pg_id page_id, seastar::temporary_buffer<char> page_data);

    // CRC的判断放在buffer pool
    // 从磁盘读取对应的page
    seastar::future<seastar::temporary_buffer<char>> ReadPage(std::string db, uint32_t table_id, std::string fi, pg_id page_id);

    // 写日志
    seastar::future<> WriteLog(temporary_buffer<char> log_data);

    
    // 读取日志
    future<std::shared_ptr<LogStream>> ReadLog(uint64_t);
    
    future<lsn_t> GetCheckPointBeginLSN();
    future<> SetCheckPointBeginLSN(lsn_t lsn);
};

}
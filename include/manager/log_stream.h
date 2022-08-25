#pragma once
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <stdint.h>
#include "../backend/type_define.h"
#include "include/manager/disk_manager.h"
namespace RawDB {
using namespace seastar;

class LogStream {
private:
    file logfile;
    input_stream<char> logstream;
    uint32_t cur_log_index;
    uint32_t cur_log_len;
    uint32_t read_log_len;
    bool is_end;
    bool file_open;
    future<> CheckIfNextLogFile();
    
public:
    static constexpr uint32_t LOGFILEHEADSIZE = 4096;
    LogStream(std::string logname, uint32_t offset, uint32_t log_index);

    ~LogStream();
    
    LogStream(LogStream&&)=default;
    LogStream& operator=(LogStream&&) = default;
    LogStream(LogStream&)=delete;
    LogStream& operator=(LogStream&) = delete;
    
    // 可能放回的buf.size() < len，这是正常情况
    // 当返回的buf.size() == 0时，表示读到了log末尾
    future<temporary_buffer<char>> Read(uint32_t len);
    
};
}


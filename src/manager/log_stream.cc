#include <include/manager/log_stream.h>

namespace RawDB {
    
    LogStream::LogStream(std::string logname, uint32_t offset, uint32_t log_index)
        : cur_log_index{log_index}, is_end{true}, file_open{false} {

        logfile = open_file_dma(logname, open_flags::ro).get0();
        file_open = true;

        DiskManager disk;
        cur_log_len = disk.GetLogFileLen(logfile).get0();
        read_log_len = 0;
        if (cur_log_len > 0) {
            is_end = false;
        }
        logstream = std::move(make_file_input_stream(logfile, offset + LOGFILEHEADSIZE));
    }
    
    LogStream::~LogStream() {
        if (file_open) {
            logstream.close().get0();
            logfile.close().get0();
        }
        
        
    }
 
    future<> LogStream::CheckIfNextLogFile() {
        assert(read_log_len <= cur_log_len);
        if (read_log_len == cur_log_len) {
            cur_log_index++;

            DiskManager disk;
            std::string logname = disk.gen_logname(cur_log_index);
            return logstream.close().then([this](){
                return logfile.close();
            }).then([this, logname = std::move(logname)](){
                file_open = false;
                return file_exists(logname).then([this, logname = std::move(logname)](bool exist){
                    if (!exist) {
                        is_end = true;
                        return make_ready_future<>();
                    }
                    return open_file_dma(logname, open_flags::ro).then([this](file f){
                            DiskManager d;
                            return d.GetLogFileLen(f).then([this, f](uint32_t len){
                            cur_log_len = len;
                            read_log_len = 0;
                            if (cur_log_len <= 0) {
                                is_end = true;
                                return make_ready_future<>();
                            }
                            
                            logstream = std::move(make_file_input_stream(f, LOGFILEHEADSIZE));
                            
                            
                            
                        }).finally([f, this](){
                            logfile = f;
                            file_open = true;
                        });
                    });
                });
            });
        }
        return make_ready_future<>();
    }
    future<temporary_buffer<char>> LogStream::Read(uint32_t len) {
        if (is_end) return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>());
        if (read_log_len + len > cur_log_len) {
            len = cur_log_len - read_log_len;
        }
        return logstream.read_up_to(len).then([this, len](temporary_buffer<char> buf){
            if (buf.size() > 0) {
                read_log_len += buf.size();
                return CheckIfNextLogFile().then([buf = std::move(buf)]()mutable{
                    return buf.share(); 
                });
                
            }
            return CheckIfNextLogFile().then([this, len](){
                return Read(len);
            });
            
        });
    }

}
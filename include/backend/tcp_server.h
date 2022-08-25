#pragma once

#include <iostream>
#include <stdint.h>
#include <seastar/net/api.hh>
#include <seastar/core/seastar.hh>
#include <include/backend/hashtable.h>
#include <include/backend/sys_cache.h>
#include <include/protocol/flatbuf.h>
#include <include/backend/kv_server.h>
#include <seastar/util/log.hh>

using namespace seastar;

namespace RawDB {


const int table_num_per_db = 100;

class sharded_server {
private:
    distributed<kv_server>& _peers;
    
    inline
    shard_id get_cpu_id(const uint32_t& tab_id) {
        return tab_id / (table_num_per_db / smp::count);
    }
    
public:
    sharded_server(distributed<kv_server>& peers) : _peers(peers) {
      //  _peers.invoke_on_all(&RawDB::kv_server::init);
    }

    future<bool> add(item_insertion_data insertion) {
        auto cpu = get_cpu_id(insertion.tab_id);std::cout << "invoke on cpu : " << cpu << std::endl  << std::endl;
        if (this_shard_id() == cpu) {
            //这里move过去应该就不用负责生存周期的问题了
            return _peers.local().add(std::move(insertion));
        }
        return _peers.invoke_on(cpu, &kv_server::add, std::move(insertion));
    }

    future<bool> replace(item_insertion_data insertion) {
        auto cpu = get_cpu_id(insertion.tab_id);std::cout << "invoke on cpu : " << cpu << std::endl  << std::endl;
        if (this_shard_id() == cpu) {
            return _peers.local().replace(std::move(insertion));
        }
        return _peers.invoke_on(cpu, &kv_server::replace, std::move(insertion));
    }

    future<bool> remove(item_insertion_data item) {
        auto cpu = get_cpu_id(item.tab_id);std::cout << "invoke on cpu : " << cpu << std::endl  << std::endl;
        if (this_shard_id() == cpu) {
            return _peers.local().remove(std::move(item));
        }
        return _peers.invoke_on(cpu, &kv_server::remove, std::move(item));
    }

    future<std::string> get(item_insertion_data item) {
        auto cpu = get_cpu_id(item.tab_id);std::cout << "invoke on cpu : " << cpu << std::endl  << std::endl;
        if (this_shard_id() == cpu) {
            return _peers.local().get(std::move(item));
        }
        return _peers.invoke_on(cpu, &kv_server::get, std::move(item));
    }

};



class raw_protocol {
private:
    enum cmd : int {
        put = 1,
        get,
        remove,
        replace
    };
    std::unordered_map<std::string, int> cmd_string_to_int {
        {"put", 1},
        {"get", 2},
        {"remove", 3},
        {"replace", 4},
    };

private:
    static constexpr const char *msg_stored = "STORED\r\n";
    static constexpr const char *msg_not_stored = "NOT_STORED\r\n";

private:
    sharded_server& _server;
    item_insertion_data _insertion;
    item_key _item_key;
    string_message _parser;
    
private:
    inline
    uint32_t get_table_id(seastar::sstring key) {
        return std::hash<sstring>()(key) % table_num_per_db;
    }

public:
    raw_protocol(sharded_server& _server) : _server(_server) {};

    void init_insertion() {
        _insertion.db_id = std::move(_parser.db);
        _insertion.tab_id = get_table_id(_parser.key);
        _insertion.key = std::move(_parser.key);
        _insertion.value = std::move(_parser.value);
    }

    future<> handle(input_stream<char>& in, output_stream<char>& out) {
        /**
         * 这里的意思是先接受4字节的len长度，进行解析
         * 然后根据得到的len长度获取len长的flatbuf对象
         * 然后进行解包得到_parser对象
         */
        return in.read().then([this, &in, &out] (temporary_buffer<char> buf){
            if(buf.size() == 0) {
                return make_ready_future<>();
            }
            std::cout << "receive size: " << buf.size() << std::endl;
            _parser.server_deserialize(std::move(buf));
            std::cout << "command is: " <<  _parser.commd << std::endl;
            switch(cmd_string_to_int[_parser.commd]) {
                case RawDB::raw_protocol::cmd::put: {
                    init_insertion();
                    return _server.add(std::move(_insertion)).then([&out] (bool added) {
                        //write应该调用temproray buffer的版本，涉及到flatbuffer打包问题，后续修改
                        return out.write(added ? msg_stored : msg_not_stored);
                    });
                }
                case RawDB::raw_protocol::cmd::get: {
                    init_insertion();
                    return _server.get(std::move(_insertion)).then([&out] (std::string val) {
                        return out.write(val);
                    });
                }
                case RawDB::raw_protocol::cmd::remove: {

                }
                case RawDB::raw_protocol::cmd::replace: {

                }
            };
        });

//demo
// std::cout << "begin read" << std::endl;
//         return in.read().then([this, &in, &out] (temporary_buffer<char> buf) {
//             if(buf.size() == 0) {
//                 std::cout << "client shut down" << std::endl;
//                 return make_ready_future<>();
//             }
                
//             std::cout << "begin trans" << std::endl;
//             std::string ss(buf.get());
            
//             std::cout << ss << std::endl;
//             std::cout << "trans ok" << std::endl;
//             return out.write("ok").then([&] {
//                 return out.flush();
//             });
//         });
//


    }

};

class tcp_server {
private:
    std::optional<future<>> _task;
    seastar::lw_shared_ptr<seastar::server_socket> _listener;
    sharded_server& _server;
    uint16_t _port;
    struct connection {
        connected_socket _socket;
        socket_address _addr;
        input_stream<char> _in;
        output_stream<char> _out;
        raw_protocol _proto;
       
        connection(connected_socket&& socket, socket_address addr, sharded_server& _server)
            : _socket(std::move(socket))
            , _addr(addr)
            , _proto(_server)
        {
            _in = _socket.input();
            _out = _socket.output();
        }

        ~connection() {}
    };
public:
    tcp_server(sharded_server& _server, uint16_t port = 11211)
        : _server(_server)
        , _port(port)
    {}

    void start() {
        listen_options lo;
        lo.reuse_address = true;
        _listener = seastar::server_socket(seastar::listen(make_ipv4_address({_port}), lo));
        // Run in the background until eof has reached on the input connection.
        _task = keep_doing([this] {
            return _listener->accept().then([this] (accept_result ar) mutable {
                connected_socket fd = std::move(ar.connection);
                socket_address addr = std::move(ar.remote_address);
                auto conn = make_lw_shared<connection>(std::move(fd), addr, _server);
                (void)do_until([conn] { return conn->_in.eof(); }, [conn] {
                    return conn->_proto.handle(conn->_in, conn->_out).then([conn] {
                        return conn->_out.flush();
                    });
                }).finally([conn] {
                    return conn->_out.close().finally([conn]{});
                });
            });
        });
    }

    future<> stop() {
        _listener->abort_accept();
        return _task->handle_exception([](std::exception_ptr e) {
            std::cerr << "exception in tcp_server " << e << '\n';
        });
    }
};


}
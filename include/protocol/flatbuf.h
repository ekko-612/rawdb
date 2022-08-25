#pragma once

#include "protocol_generated.h"
#include <seastar/core/ragel.hh>
#include <include/backend/kv_server.h>
/**
 * 发送时可以先发送4字节的len长度，表示接下来的这个flatbuffer对象大小
 * 然后通过解析这4字节的len长度进行后续的接收并解包
 * 
 */
namespace RawDB {

class message {
public:
    std::string commd;
    std::string db;
    std::string key;

public:
    message() = default;

    message(std::string commd, std::string db, std::string key)
        : commd(commd)
        , db(db)
        , key(key)
    {};
    
    // template<typename UnionTypeBuilder>
    // seastar::temporary_buffer<char> pack_message(UnionTypeBuilder&& utb, messageBody type, std::string command, std::string db, std::string key){
    //     auto un = utb.Finish().Union();
    //     flatObjBuilder fob(utb.fbb_);
    //     auto cmd_flbf = fob.fbb_.CreateString(command);
    //     fob.add_commd(cmd_flbf);
    //     auto db_flbf = fob.fbb_.CreateString(db);
    //     fob.add_db(db_flbf);
    //     auto key_flbf = fob.fbb_.CreateString(key);
    //     fob.add_key(key_flbf);
    //     fob.add_message_type(type);
    //     fob.add_message(un);
    //     utb.fbb_.FinishSizePrefixed(fob.Finish());
    //     return flatbuffers_as_buffer(std::move(utb.fbb_));
    // }

    inline seastar::temporary_buffer<char> flatbuffers_as_buffer(flatbuffers::FlatBufferBuilder&& fbb)
    {
        auto mem = fbb.Release();
        auto data = reinterpret_cast<char*>(mem.data());
        auto size = mem.size();
        return seastar::temporary_buffer<char>(data, size, seastar::make_object_deleter(std::move(mem)));
    }
};//end of class message

// class bool_message : public message
// {
// public:
//     bool value;

//     bool_message() = default;
    
//     bool_message(std::string commd, std::string db, std::string key, bool value)
//         : message(commd, db, key)
//         , value(value)
//     {};

//     void init() {
//         // commd.reserve(8);
//         // key.reserve(16);
//         // value.reserve(16);

//         //for test
//         commd = "put";
//         db = "0";
//         key = "test";
//         value = "test_val";
//     }

    
//     seastar::temporary_buffer<char> server_serialize() {
//         flatbuffers::FlatBufferBuilder fbb;
//         auto res = bool_tableBuilder(fbb);
//         res.add_value(value);
//         return pack_message(std::move(res), messageBody_message_bool, commd, db, key);      
//     }

//     void server_deserialize(seastar::temporary_buffer<char>&& buf) {
//         RawDB::page_object_wrapper<flatbuffers::FlatBufferBuilder> builder(std::move(buf));

//         char* ptr = (char*)builder->GetBufferPointer();
//         uint64_t size = builder->GetSize();
//         auto obj = RawDB::GetflatObj((uint8_t*)ptr);

//         commd = obj->commd()->c_str();
//         db = obj->db()->c_str();
//         key = obj->key()->c_str();
//         value = obj->message_as_message_bool()->value();
//     }
// };//end of class bool_message

class string_message : public message
{
public:
    std::string value;

    string_message() = default;
    
    string_message(std::string commd, std::string db, std::string key, std::string value)
        : message(commd, db, key)
        , value(value)
    {};

    void init() {
        // commd.reserve(8);
        // key.reserve(16);
        // value.reserve(16);
    }

    // seastar::temporary_buffer<char> server_serialize() {
    //     flatbuffers::FlatBufferBuilder fbb;
    //     auto res = string_tableBuilder(fbb);
    //     auto val = fbb.CreateString(value);
    //     res.add_value(val);
    //     return pack_message(std::move(res), messageBody_message_string, commd, db, key);      
    // }

    // void server_deserialize(seastar::temporary_buffer<char>&& buf) {
    //     RawDB::page_object_wrapper<flatbuffers::FlatBufferBuilder> builder(std::move(buf));

    //     char* ptr = (char*)builder->GetBufferPointer();
    //     uint64_t size = builder->GetSize();
    //     auto obj = RawDB::GetflatObj((uint8_t*)ptr);

    //     commd = obj->commd()->c_str();
    //     db = obj->db()->c_str();
    //     key = obj->key()->c_str();
    //     value = obj->message_as_message_string()->value()->c_str();
    // }

    seastar::temporary_buffer<char> server_serialize() {
        flatbuffers::FlatBufferBuilder fbb;
        auto cmd = fbb.CreateString(commd);
        auto d = fbb.CreateString(db);
        auto k = fbb.CreateString(key);
        auto v = fbb.CreateString(value);

        auto mloc = CreateflatObj(fbb, cmd, d, k, v);
        fbb.Finish(mloc);
        char* ptr = (char*)fbb.GetBufferPointer();
	    uint64_t size = fbb.GetSize();
      
        return flatbuffers_as_buffer(std::move(fbb));      
    }

    void server_deserialize(seastar::temporary_buffer<char>&& buf) {
        std::cout << "buf size is : " << buf.size() << std::endl;
        flatbuffers::Verifier verifier((const uint8_t *)buf.get(), buf.size());
        if (!VerifyflatObjBuffer(verifier))
        {
            std::cerr << "error data" << std::endl;
            return;
        }
        // 解析
        auto obj = GetflatObj(buf.get_write());


        //std::cout << "?" << buf.size() << std::endl;
//         RawDB::page_object_wrapper<flatbuffers::FlatBufferBuilder> builder(std::move(buf));
// std::cout << "?" << std::endl;
//         char* ptr = (char*)builder->GetBufferPointer();std::cout << "??" << std::endl;
//         uint64_t size = builder->GetSize();
//         auto obj = RawDB::GetflatObj((uint8_t*)ptr);
// std::cout << "???" << std::endl;
        commd = obj->commd()->c_str();
        db = obj->db()->c_str();
        key = obj->key()->c_str();
        value = obj->value()->c_str();
    }
};//end of class string_message

}//end of namespace RawDB
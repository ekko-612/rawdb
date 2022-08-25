#pragma once

#include <stdint.h>
#include <string>
#include <unordered_map>
#include <list>
#include <seastar/core/units.hh>

using namespace seastar;

namespace RawDB {
    
using db_id = std::string;
using tab_id = uint32_t;
using seg_id = uint32_t;
using bkt_id = uint32_t;
using pg_id = uint64_t;
using pg_offset = uint16_t;
using key_sz = uint32_t;
using bmp_id = uint64_t;
using lsn_t = uint64_t;

};
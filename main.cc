#include <iostream>
#include <stdint.h>
#include <include/backend/type_define.h>
#include <include/backend/tcp_server.h>
#include <include/backend/kv_server.h>
#include <include/backend/sys_cache.h>
#include <include/manager/disk_manager.h>
#include <include/backend/hashfunc.h>
#include <include/backend/hashtable.h>
#include <include/stroage/bkt_page.h>
#include <include/stroage/data_page.h>
#include <include/stroage/seg_page.h>
#include <seastar/core/units.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/api.hh>
#include <seastar/core/seastar.hh>
#include <unistd.h>
#include <iomanip>

using namespace seastar;

#define PLATFORM "seastar"
#define VERSION "v1.0"

namespace RawDB {
       
// static constexpr double default_slab_growth_factor = 1.25;
// static constexpr uint64_t default_slab_page_size = 1UL* MB;
// static constexpr uint64_t default_per_cpu_slab_size = 0UL; // zero means reclaimer is enabled.

}

int main(int ac, char** av) {

    distributed<RawDB::kv_server> servers;
    RawDB::sharded_server server(servers);
    distributed<RawDB::tcp_server> tcp_server;

    namespace bpo = boost::program_options;    

    app_template app;
    app.add_options()
        // ("max-slab-size", bpo::value<uint64_t>()->default_value(RawDB::default_per_cpu_slab_size/MB),
        //      "Maximum memory to be used for items (value in megabytes) (reclaimer is disabled if set)")
        // ("slab-page-size", bpo::value<uint64_t>()->default_value(RawDB::default_slab_page_size/MB),
        //      "Size of slab page (value in megabytes)")
        ("port", bpo::value<uint16_t>()->default_value(11211),
             "Specify UDP and TCP ports for memcached server to listen on")
        ;

    return app.run_deprecated(ac, av, [&] {
        
        engine().at_exit([&] { return tcp_server.stop(); });
        engine().at_exit([&] { return servers.stop(); });

        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        // uint64_t per_cpu_slab_size = config["max-slab-size"].as<uint64_t>() * MB;
        // uint64_t slab_page_size = config["slab-page-size"].as<uint64_t>() * MB;
        //return servers.start(std::move(per_cpu_slab_size), std::move(slab_page_size)).then([&] {
     
     
        return servers.start().then([&] {
            std::cout << PLATFORM << " rawdb " << VERSION << "\n";
            return make_ready_future<>();
        }).then([&, port] {
            return tcp_server.start(std::ref(server), port);
        }).then([&tcp_server, &servers] {
            return servers.invoke_on_all(&RawDB::kv_server::init).then([&] {
                return tcp_server.invoke_on_all(&RawDB::tcp_server::start);
            });
        });

 //for Test
        
         //RawDB::kv_server svr;
        // RawDB::item_key key("0");
        // RawDB::item_insertion_data inser("0", 0, std::move(key), "world");
        
        // return svr.add(std::move(inser)).then([&svr] (bool res) {
        //     std::cout << (res ? "true 1" : "false 1") << std::endl;
        //     RawDB::item_key key1("1");
        //     RawDB::item_insertion_data inser1("0", 0, key1, "world!");
            
        //     return svr.add(std::move(inser1)).then([] (bool ans) {
        //         std::cout << (ans ? "true 2" : "false 2") << std::endl;
        //     });
        // });


        // svr.test_get_table_dir();
        // return svr.test_CreateDB();
       


    });
}
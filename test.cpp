#include <iostream>
#include <thread>
#include <list>
#include "shared_memory_object.hpp"
#include "s3_multipart_shared_data.hpp"

int main() {

    using multipart_shared_data =
        irods::experimental::io::s3_transport::shared_data::multipart_shared_data;
    using named_shared_memory_object =
        irods::experimental::interprocess::shared_memory::named_shared_memory_object
        <multipart_shared_data>;
    namespace types = irods::experimental::io::s3_transport::shared_data::interprocess_types;

    named_shared_memory_object obj{"helloworld", 900, 65536};
    // resizing vector
    obj.atomic_exec([&obj](auto& _obj) {
            _obj.upload_id = "abc";
            _obj.etags.resize(2, types::shm_char_string("", obj.get_allocator()));
            _obj.etags[0] = "123";
            _obj.etags[1] = "456";
    });
    named_shared_memory_object obj2{"helloworld", 900, 65536};

    // getting a value
    auto val = obj2.atomic_exec([](auto& _obj) {
            return _obj.upload_id;
    });
    std::cout << val << std::endl;

    // iterating over vector
    obj.atomic_exec([](auto& _obj) {
            for (auto const& iter : _obj.etags) {
                std::cout << iter << std::endl;
            }
    });

    // testing multithreaded / locking
    std::vector<std::thread*> threads;
    for (int i = 0; i < 10; ++i) {
        std::thread *this_thread = new std::thread([&obj] () {
                for (int j = 0; j < 100; ++j) {
                    obj.atomic_exec([](auto& _obj) {
                            (_obj.file_open_counter)++;
                    });
                }
        });
        threads.push_back(this_thread);

    }

    for (int i = 0; i < 10; ++i) {
        std::thread *this_thread = threads[i];
        this_thread->join();
        delete this_thread;
    }


    // getting the value after increments
    auto cntr = obj.atomic_exec([](auto& _obj) {
            return _obj.file_open_counter;
    });
    std::cout << cntr << std::endl;



    obj.remove();


    //using shared_memory_object = irods::experimental::interprocess::shared_memory_object<fd_info>
    return 0;
}

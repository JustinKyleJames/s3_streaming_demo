#include <iostream>
#include <thread_pool.hpp>
#include <boost/chrono.hpp>


int main() {

    const unsigned int THREAD_COUNT = 5;

    irods::thread_pool threads{THREAD_COUNT};

    for (unsigned int i = 0; i < THREAD_COUNT; ++i) {

        irods::thread_pool::post(threads, [i] () {
                std::cout << "thread " << i << " start" << std::endl << std::flush;
                boost::this_thread::sleep_for(boost::chrono::milliseconds(i * 1000));
                std::cout << "thread " << i << " finish" << std::endl << std::flush;
        });

    }

    std::cout << "waiting for threads to finish..." << std::endl;
    threads.stop();
    threads.join();
    std::cout << "done waiting" << std::endl;

    return 0;
}

#include <thread_pool.hpp>
#include <boost/chrono.hpp>
#include <stdio.h>


int main() {

    const unsigned int THREAD_COUNT = 5;

    irods::thread_pool threads{THREAD_COUNT};

    for (unsigned int i = 0; i < THREAD_COUNT; ++i) {

        irods::thread_pool::post(threads, [i] () {
                printf("[%u] running\n", i);
                fflush(stdout);
                boost::this_thread::sleep_for(boost::chrono::milliseconds(i * 1000));
                printf("[%u] finishing\n", i);
                fflush(stdout);
        });

    }

    printf("waiting for threads to finish\n");
    fflush(stdout);
    threads.stop();
    threads.join();
    printf("done waiting\n");
    fflush(stdout);

    return 0;
}

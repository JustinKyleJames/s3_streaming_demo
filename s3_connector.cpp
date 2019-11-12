#include <iostream>
#include <stdint.h>

#include <boost/circular_buffer.hpp>

#include <condition_variable>
#include <thread>
#include <stdlib.h>
#include <chrono>


// individual request
struct upload_page_t {
   size_t buffer_size;
   uint8_t *buffer;
};  


// ring buffer with protection for overwrites 
template <typename T>
class ring_buffer {

  public:

    ring_buffer(size_t size) {
        cb.set_capacity(size);
    }

    ~ring_buffer() {
    }

    void read(T& entry) {
        {
            std::unique_lock<std::mutex> lk(cv_mutex);
            //std::cout << "Read wait" << std::endl;
            cv.wait(lk, [this] {
                    std::cout << "read woke up, test=" << (cb.size() > 0) << std::endl; 
                    return 0 < cb.size();});
            auto iter = cb.begin();
            entry = *iter;
            cb.pop_front(); 
            //std::cout << "Read: size=" << cb.size() << std::endl;
            std::cout << "Read: " << entry << " [" << cb.size() << ", " << cb.capacity() << "]" << std::endl;
            //std::cout << "Read notify_one" << std::endl;
        }
        cv.notify_one();
    } 

    void write(const T& entry) {
        {
            std::unique_lock<std::mutex> lk(cv_mutex);
            //std::cout << "Write wait" << std::endl;
            cv.wait(lk, [this] {
                    std::cout << "write woke up, test=" << (cb.size() < cb.capacity()) << std::endl; 
                    return cb.size() < cb.capacity();});
            cb.push_back(entry);
            //std::cout << "Write: size=" << cb.size() << std::endl;
            std::cout << "Write: " << entry << " [" << cb.size() << ", " << cb.capacity() << "]" << std::endl;
            //std::cout << "Write notify_one" << std::endl;
        }
        cv.notify_one();
    }

    size_t get_number_entries() {
        std::unique_lock<std::mutex> lk(cv_mutex);
        return cb.size();
    }

  private:

    boost::circular_buffer<T> cb;
    std::condition_variable cv;
    std::mutex cv_mutex;
};

std::mutex start_mtx;
std::condition_variable start_cv;
bool ready = false;

std::mutex read_cntr_mutex;
std::mutex write_cntr_mutex;
int read_counter = 0;
int write_counter = 0;

void busy_wait(int seconds) {
    using namespace std::chrono;
    int start = duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
    while (true) {
        int now = duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
        if (now - start > seconds * 1000) {
            break;
        }
    }
}

void write_loop(ring_buffer<int> *buffer) {

    // sync start
    {
        std::unique_lock<std::mutex> lck(start_mtx);
        while (!ready) {
            start_cv.wait(lck);
        }
    }

    for (int i = 0; i < 50; ++i) {
        buffer->write(i);
        {
            std::lock_guard<std::mutex> cntr_lock(write_cntr_mutex);
            write_counter++;
        }

        busy_wait(rand() % 3);

    }


}

void read_loop(ring_buffer<int> *buffer) {

    // sync start
    {
        std::unique_lock<std::mutex> lck(start_mtx);
        while (!ready) {
            start_cv.wait(lck);
        }
    }

    for (int i = 0; i < 50; ++i) {
        int val;
        buffer->read(val);
        {
            std::lock_guard<std::mutex> cntr_lock(read_cntr_mutex);
            read_counter++;
        }
        busy_wait(rand() % 5);
    }



}
void go() {
  std::unique_lock<std::mutex> lck(start_mtx);
  ready = true;
  start_cv.notify_all();
}

int main() {

    const size_t THREAD_COUNT = 5;

    ring_buffer<int> buffer(10); 

    std::thread reader[THREAD_COUNT];
    std::thread writer[THREAD_COUNT];

    go();

    for (int i = 0; i < THREAD_COUNT; ++i) {
        writer[i] = std::thread(write_loop, &buffer);
        reader[i] = std::thread(read_loop, &buffer);
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
        writer[i].join();
        reader[i].join();
    }
    
    std::cout << "total write: " << write_counter << std::endl;
    std::cout << "total read:  " << read_counter << std::endl;

    return 0;
}



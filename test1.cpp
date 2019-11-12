#include <iostream>
#include <stdint.h>

#include <boost/circular_buffer.hpp>

#include <condition_variable>
#include <thread>


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
                    std::cout << "Read woke up, test=" << (cb.size() > 0) << std::endl; 
                    return 0 < cb.size();});
            auto iter = cb.begin();
            entry = *iter;
            cb.pop_front(); 
            std::cout << "Read: size=" << cb.size() << std::endl;
            //std::cout << "Read: " << entry << " [" << cb.size() << ", " << cb.capacity() << "]" << std::endl;
            std::cout << "Read notify_one" << std::endl;
        }
        cv.notify_one();
    } 

    void read2(T& entry) {
        {
            std::unique_lock<std::mutex> lk(cv_mutex);
            //std::cout << "Read wait" << std::endl;
            cv.wait(lk, [this] {
                    std::cout << "Read2 woke up, test=" << false << std::endl; 
                    return false;});
            auto iter = cb.begin();
            entry = *iter;
            cb.pop_front(); 
            std::cout << "Read2: size=" << cb.size() << std::endl;
            //std::cout << "Read: " << entry << " [" << cb.size() << ", " << cb.capacity() << "]" << std::endl;
            std::cout << "Read2 notify_one" << std::endl;
        }
        cv.notify_one();
    } 
    void write(const T& entry) {
        {
            std::unique_lock<std::mutex> lk(cv_mutex);
            //std::cout << "Write wait" << std::endl;
            cv.wait(lk, [this] {
                    std::cout << "Write woke up, test=" << (cb.size() < cb.capacity()) << std::endl; 
                    return cb.size() < cb.capacity();});
            cb.push_back(entry);
            std::cout << "Write: size=" << cb.size() << std::endl;
            //std::cout << "Write: " << entry << " [" << cb.size() << ", " << cb.capacity() << "]" << std::endl;
            std::cout << "Write notify_one" << std::endl;
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

void write_loop(ring_buffer<int> *buffer) {

    // sync start
    {
        std::unique_lock<std::mutex> lck(start_mtx);
        while (!ready) {
            start_cv.wait(lck);
        }
    }

    for (int i = 0; i < 50000; ++i) {
        buffer->write(i);
        //sleep(1);
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

    for (int i = 0; i < 50000; ++i) {
        int val;
        buffer->read(val);
    }

}

void read_loop2(ring_buffer<int> *buffer) {

    // sync start
    {
        std::unique_lock<std::mutex> lck(start_mtx);
        while (!ready) {
            start_cv.wait(lck);
        }
    }

    for (int i = 0; i < 50000; ++i) {
        int val;
        buffer->read2(val);
    }

}
void go() {
  std::unique_lock<std::mutex> lck(start_mtx);
  ready = true;
  start_cv.notify_all();
}

int main() {

    ring_buffer<int> buffer(1000); 
    std::thread writer(write_loop, &buffer);
    std::thread writer2(write_loop, &buffer);
    std::thread reader(read_loop, &buffer);
    std::thread reader2(read_loop2, &buffer);
    go();

    writer.join();
    writer2.join();
    reader.join();
    reader2.join();

    return 0;
}



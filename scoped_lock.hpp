#ifndef SCOPED_LOCK_HPP
#define SCOPED_LOCK_HPP

#include <boost/interprocess/sync/named_mutex.hpp>

namespace irods::experimental::io
{

    // TODO move somewhere else
    class scoped_lock
    {
    public:

        scoped_lock(const std::string& key,
                    const std::string& _file = "",
                    int _line = 0,
                    const std::string& _function = "",
                    int _object_identifier = 0)
            : file{_file}
            , line{_line}
            , function{_function}
            , named_mutex_ptr{nullptr}
            , lock_ptr{nullptr}
            , object_identifier{_object_identifier}
        {
            namespace bi = boost::interprocess;

            auto mutex_name = key + MULTIPART_SHARED_MUTEX_EXTENSION;

            //if (file != nullptr && function != nullptr) {
            if (file != "" && function != "") {
                printf("%s:%d (%s) [[%d]] ---LOCK--- waiting for lock\n",
                        file.c_str(), line, function.c_str(), object_identifier);
            }

            //named_mutex_ptr = new bi::named_mutex(bi::open_or_create, mutex_name.c_str());
            named_mutex_ptr = std::shared_ptr<bi::named_mutex>(new bi::named_mutex(bi::open_or_create, mutex_name.c_str()));
            lock_ptr = std::make_unique<bi::scoped_lock<bi::named_mutex>>(
                    bi::scoped_lock<bi::named_mutex>(*named_mutex_ptr));

            //lock = new bi::scoped_lock<bi::named_mutex>(*named_mutex);

            //if (file != nullptr && function != nullptr) {
            if (file != "" && function != "") {
                printf("%s:%d (%s) [[%d]] ---LOCK--- acquired lock\n",
                        file.c_str(), line, function.c_str(), object_identifier);
            }
        }

        ~scoped_lock()
        {
            //if (file != nullptr && function != nullptr) {
            if (file != "" && function != "") {
                printf("%s:%d (%s) [[%d]] ---LOCK--- releasing lock\n",
                        file.c_str(), line, function.c_str(), object_identifier);
            }

        }

        scoped_lock(scoped_lock&&) = delete;
        scoped_lock(const scoped_lock&) = delete;
        scoped_lock& operator=(const scoped_lock&) = delete;
        scoped_lock& operator=(scoped_lock&&) = delete;

        void lock()
        {
            lock_ptr->lock();
        }

        void unlock()
        {
            lock_ptr->unlock();
        }

    private:

        const std::string MULTIPART_SHARED_MUTEX_EXTENSION{"-mtx"};

        // TODO shared_ptr
        //boost::interprocess::named_mutex *named_mutex_ptr;
        std::shared_ptr<boost::interprocess::named_mutex> named_mutex_ptr;
        std::unique_ptr<boost::interprocess::scoped_lock<boost::interprocess::named_mutex>> lock_ptr;
        const std::string file;
        int line;
        const std::string function;
        int object_identifier;
    };
} // end namespace irods::experimental::io

#endif // SCOPED_LOCK_TEST_HPP

#ifndef IRODS_SHARED_MEMORY_OBJECT_HPP
#define IRODS_SHARED_MEMORY_OBJECT_HPP

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/container/scoped_allocator.hpp>

#include <string>
#include <utility>

#include "scoped_lock.hpp"

namespace irods::experimental::interprocess
{
    namespace shared_memory
    {
        namespace bi = boost::interprocess;
        namespace bc = boost::container;

        using segment_manager       = bi::managed_shared_memory::segment_manager;
        using void_allocator        = bc::scoped_allocator_adaptor<bi::allocator<void, segment_manager>>;

        template <typename T>
        class named_shared_memory_object
        {
        private:
            struct ipc_object
            {
                ipc_object(void_allocator&& alloc_inst)
                    : thing{alloc_inst}
                {}

                // T must have reset_fields()
                T thing;

                bi::interprocess_mutex mtx;
                time_t last_access_time_in_seconds;
            };

        public:

            template <typename ...Args>
            named_shared_memory_object(std::string shm_name,
                    time_t shared_memory_timeout_in_seconds,
                    uint64_t shm_size,
                    Args&& ..._args)

                : shm_name_{shm_name}
                , shm_size_{shm_size}
                , shm_{bi::open_or_create, shm_name_.c_str(), shm_size_}
                , alloc_inst_{shm_.get_segment_manager()}

            {

                object_ = shm_.find_or_construct<ipc_object>
                    (SHARED_DATA_NAME.c_str())(static_cast<void_allocator>(shm_.get_segment_manager()));

                const time_t now = time(0);

                const bool shmem_has_expired = now - object_->last_access_time_in_seconds
                    > shared_memory_timeout_in_seconds;

                if (shmem_has_expired) {
                    object_->thing.reset_fields();
                }

                object_->last_access_time_in_seconds = now;

            }

            auto remove() -> void
            {
                object_->thing.~T();
                bi::shared_memory_object::remove(shm_name_.c_str());
            }

            template <typename Function>
            auto atomic_exec(Function _func) const
            {
                bi::scoped_lock lk{object_->mtx};
                return _func(object_->thing);
            }

            template <typename Function>
            auto exec(Function _func) const
            {
                return _func(object_->thing);
            }

            void_allocator& get_allocator() {
                return alloc_inst_;
            }

        private:

            const std::string shm_name_;
            const uint64_t shm_size_;
            bi::managed_shared_memory shm_;
            void_allocator alloc_inst_;
            ipc_object* object_;
            const std::string SHARED_DATA_NAME{"SharedData"};

        }; // class shared_memory_object

    } // namespace shared_memory
} // namespace irods::experimental::ipc

#endif // IRODS_SHARED_MEMORY_OBJECT_HPP

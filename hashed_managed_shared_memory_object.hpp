#ifndef HASHED_MANAGED_SHARED_MEMORY_OBJECT_HPP
#define HASHED_MANAGED_SHARED_MEMORY_OBJECT_HPP

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/container/scoped_allocator.hpp>

#include <string>
#include <utility>

#include "managed_shared_memory_object.hpp"

namespace irods::experimental::interprocess
{
    namespace shared_memory
    {

        template <typename T>
        class hashed_named_shared_memory_object : public named_shared_memory_object<T>
        {

        public:

            hashed_named_shared_memory_object(const hashed_named_shared_memory_object&) = delete;

            auto operator=(const hashed_named_shared_memory_object&) ->
                hashed_named_shared_memory_object& = delete;

            template <typename... Args>
            hashed_named_shared_memory_object(std::string shm_name,
                                              time_t shared_memory_timeout_in_seconds,
                                              uint64_t shm_size,
                                              Args&& ...args)
                : named_shared_memory_object<T>{std::to_string(std::hash<std::string>{}(shm_name)),
                                             shared_memory_timeout_in_seconds,
                                             shm_size,
                                             std::forward<Args>(args)...}
            {
            }

        }; // class hashed_shared_memory_object

    } // namespace shared_memory

} // namespace irods::experimental::interprocess

#endif  // HASHED_MANAGED_SHARED_MEMORY_OBJECT_HPP

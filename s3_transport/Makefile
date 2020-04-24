INCL_DIRS=-I./include -I/opt/irods-externals/boost1.67.0-0/include -I/opt/irods-externals/jansson2.7-0/include -I/opt/irods-externals/json3.1.2-0/include -I/opt/irods-externals/libs3a30e55e8-1/include -I/usr/include/irods -I/usr/include/libxml2

DEBUG_INCL_DIRS=-I./include -I/home/jjames/github/libs3/build/include -I/opt/irods-externals/boost1.67.0-0/include -I/opt/irods-externals/jansson2.7-0/include -I/opt/irods-externals/json3.1.2-0/include -I/usr/include/irods -I/usr/include/libxml2

INCL_DEPS=include/circular_buffer.hpp include/managed_shared_memory_object.hpp include/s3_transport_callbacks.hpp include/s3_transport_types.hpp include/lock_and_wait_strategy.hpp include/s3_multipart_shared_data.hpp include/s3_transport.hpp include/s3_transport_util.hpp

LINK_LIBS=-L/opt/irods-externals/boost1.67.0-0/lib
DEBUG_LINK_LIBS=-L/opt/irods-externals/boost1.67.0-0/lib
RPATHS=-Wl,-rpath,/opt/irods-externals/boost1.67.0-0/lib/ -Wl,-rpath,/opt/irods-externals/clang-runtime6.0-0/lib -Wl,-rpath,/opt/irods-externals/libs3a30e55e8-1/lib
DEBUG_RPATHS=-Wl,-rpath,/home/jjames/github/libs3/build/lib -Wl,-rpath,/opt/irods-externals/boost1.67.0-0/lib/ -Wl,-rpath,/opt/irods-externals/clang-runtime6.0-0/lib



SRC_DEPS=src/s3_transport.cpp

COMPILER=/opt/irods-externals/clang6.0-0/bin/clang++

s3_transport_test: s3_transport_test.cpp $(SRC_DEPS) $(INCL_DEPS)
	$(COMPILER) $< $(SRC_DEPS) -std=c++17 -g -o $@ $(INCL_DIRS) $(LINK_LIBS) $(RPATHS) -stdlib=libc++ -lboost_system -lboost_filesystem -lcrypto -lz -lrt -pthread -lc++abi /opt/irods-externals/jansson2.7-0/lib/libjansson.a -lboost_thread /opt/irods-externals/libs3a30e55e8-1/lib/libs3.so

s3_transport_test_debug: s3_transport_test.cpp $(SRC_DEPS) $(INCL_DEPS)
	$(COMPILER) $< $(SRC_DEPS) -std=c++17 -g -o $@ $(DEBUG_INCL_DIRS) $(DEBUG_LINK_LIBS) $(DEBUG_RPATHS) -stdlib=libc++ -lboost_system -lboost_filesystem -lcrypto -lz -lrt -pthread -lc++abi /opt/irods-externals/jansson2.7-0/lib/libjansson.a -lboost_thread -lcurl -lxml2 /opt/irods-externals/libs3a30e55e8-1/lib/libs3.so

read_shared_memory: read_shared_memory.cpp $(SRC_DEPS) $(INCL_DEPS)
	$(COMPILER) $< $(SRC_DEPS) -std=c++17 -g -o $@ --std=c++17 $(INCL_DIRS) $(LINK_LIBS) $(RPATHS) -stdlib=libc++ -lboost_system -lboost_filesystem -lcrypto -lz -lrt -pthread -lc++abi /opt/irods-externals/jansson2.7-0/lib/libjansson.a -lboost_thread /opt/irods-externals/libs3a30e55e8-1/lib/libs3.so


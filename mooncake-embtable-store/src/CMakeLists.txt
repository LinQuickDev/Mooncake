# mooncake-embtable-store is built layer by layer (design doc section 8.11).
# Layers are added incrementally and verified before stacking the next one.

# Convenience variable: our own include directory.
set(EMBTABLE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# Convenience variable: mooncake-store include directory and its sub-paths that
# are needed because mooncake_store uses directory-level include_directories
# which do not propagate to dependents.
set(MOONCAKE_STORE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include)
set(MOONCAKE_STORE_INCLUDE_DIRS
    ${MOONCAKE_STORE_INCLUDE_DIR}
    ${MOONCAKE_STORE_INCLUDE_DIR}/cachelib_memory_allocator/include
    ${MOONCAKE_STORE_INCLUDE_DIR}/cachelib_memory_allocator/fake_include
    ${MOONCAKE_STORE_INCLUDE_DIR}/cachelib_memory_allocator
    ${MOONCAKE_STORE_INCLUDE_DIR}/mooncake-store/proto
    ${CMAKE_CURRENT_BINARY_DIR}/../../mooncake-store/include
)
set(MOONCAKE_TE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include)

# ---- ShareObject layer (depends on mooncake_store) ----
add_library(embtable_share_object OBJECT
    share_object/share_object.cpp
    share_object/vector_object.cpp
    share_object/index_object.cpp
    share_object/share_map_meta.cpp
    share_object/share_map.cpp
)
target_include_directories(embtable_share_object PUBLIC
    ${EMBTABLE_INCLUDE_DIR}
    ${boomphf_SOURCE_DIR}
    ${MOONCAKE_STORE_INCLUDE_DIRS}
    ${MOONCAKE_TE_INCLUDE_DIR}
)
target_link_libraries(embtable_share_object PUBLIC
    mooncake_store glog::glog UbDiag::ubdiag_lib)

# ---- ShareMapStore layer (depends on ShareObject) ----
add_library(embtable_share_map_store OBJECT
    share_map_store/share_map_store.cpp
    share_map_store/share_map_store_rpc_service.cpp
    share_map_store/share_map_store_client.cpp
)
target_include_directories(embtable_share_map_store PUBLIC
    ${EMBTABLE_INCLUDE_DIR}
    ${MOONCAKE_STORE_INCLUDE_DIRS}
    ${MOONCAKE_TE_INCLUDE_DIR}
)
target_link_libraries(embtable_share_map_store PUBLIC
    embtable_share_object UbDiag::ubdiag_lib)

# ---- EmbTable layer (depends on ShareMapStore) ----
add_library(embtable_emb_table OBJECT
    emb_table/emb_table_meta.cpp
    emb_table/emb_table_bucket.cpp
    emb_table/emb_table.cpp
)
target_include_directories(embtable_emb_table PUBLIC
    ${EMBTABLE_INCLUDE_DIR}
    ${MOONCAKE_STORE_INCLUDE_DIRS}
    ${MOONCAKE_TE_INCLUDE_DIR}
)
target_link_libraries(embtable_emb_table PUBLIC
    embtable_share_map_store UbDiag::ubdiag_lib)

# ---- EmbTableClient layer (depends on EmbTable + ShareMapStore) ----
add_library(embtable_emb_table_client OBJECT
    emb_table_client/emb_table_client.cpp
    emb_table_client/emb_table_rpc_service.cpp
    emb_table_client/emb_table_dummy_client.cpp
)
target_include_directories(embtable_emb_table_client PUBLIC
    ${EMBTABLE_INCLUDE_DIR}
    ${MOONCAKE_STORE_INCLUDE_DIRS}
    ${MOONCAKE_TE_INCLUDE_DIR}
)
target_link_libraries(embtable_emb_table_client PUBLIC
    embtable_emb_table embtable_share_map_store UbDiag::ubdiag_lib)

# ---- Final library aggregating all layers (updated as layers are added) ----
add_library(mooncake_embtable_store
    $<TARGET_OBJECTS:embtable_share_object>
    $<TARGET_OBJECTS:embtable_share_map_store>
    $<TARGET_OBJECTS:embtable_emb_table>
    $<TARGET_OBJECTS:embtable_emb_table_client>
)
target_include_directories(mooncake_embtable_store PUBLIC
    ${EMBTABLE_INCLUDE_DIR}
    ${boomphf_SOURCE_DIR}
    ${MOONCAKE_STORE_INCLUDE_DIRS}
    ${MOONCAKE_TE_INCLUDE_DIR}
)
target_link_libraries(mooncake_embtable_store PUBLIC
    mooncake_store glog::glog UbDiag::ubdiag_lib)

add_executable(embtable_client
    emb_table_client/embtable_client_main.cpp
)
target_include_directories(embtable_client PRIVATE
    ${EMBTABLE_INCLUDE_DIR}
    ${MOONCAKE_STORE_INCLUDE_DIRS}
    ${MOONCAKE_TE_INCLUDE_DIR}
)
target_link_libraries(embtable_client PRIVATE
    mooncake_embtable_store
    transfer_engine
    gflags::gflags
    glog::glog
    pthread
)

add_subdirectory(../benchmarks benchmarks)

if(BUILD_SHARED_LIBS)
    install(TARGETS mooncake_embtable_store DESTINATION lib)
    install(TARGETS embtable_client DESTINATION bin)
endif()

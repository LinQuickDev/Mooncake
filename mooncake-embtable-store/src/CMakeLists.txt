# mooncake-embtable-store is built layer by layer (design doc section 8.11).
# Layers are added incrementally and verified before stacking the next one.

# ---- ShareObject layer (depends on mooncake_store) ----
add_library(embtable_share_object OBJECT
    share_object/share_object.cpp
    share_object/vector_object.cpp
    share_object/index_object.cpp
    share_object/share_map_meta.cpp
    share_object/share_map.cpp
)
target_include_directories(embtable_share_object PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${boomphf_SOURCE_DIR}
    # mooncake_store uses directory-level include_directories which do not
    # propagate to dependents; expose the cachelib/proto paths explicitly so
    # our headers can include real_client.h -> types.h -> Slab.h, etc.
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include/cachelib_memory_allocator/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include/cachelib_memory_allocator/fake_include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include/cachelib_memory_allocator
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include/mooncake-store/proto
    ${CMAKE_CURRENT_BINARY_DIR}/../../mooncake-store/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include
)
target_link_libraries(embtable_share_object PUBLIC mooncake_store glog::glog)

# ---- ShareMapStore layer (depends on ShareObject) ----
add_library(embtable_share_map_store OBJECT
    share_map_store/share_map_store.cpp
)
target_include_directories(embtable_share_map_store PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)
target_link_libraries(embtable_share_map_store PUBLIC embtable_share_object)

# ---- EmbTable layer (depends on ShareMapStore) ----
add_library(embtable_emb_table OBJECT
    emb_table/emb_table_meta.cpp
    emb_table/emb_table_bucket.cpp
    emb_table/emb_table.cpp
)
target_include_directories(embtable_emb_table PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)
target_link_libraries(embtable_emb_table PUBLIC embtable_share_map_store)

# ---- EmbTableClient layer (depends on EmbTable + ShareMapStore) ----
add_library(embtable_emb_table_client OBJECT
    emb_table_client/emb_table_client.cpp
    emb_table_client/emb_table_dummy_client.cpp
)
target_include_directories(embtable_emb_table_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)
target_link_libraries(embtable_emb_table_client PUBLIC
    embtable_emb_table embtable_share_map_store)

# ---- Final library aggregating all layers (updated as layers are added) ----
add_library(mooncake_embtable_store
    $<TARGET_OBJECTS:embtable_share_object>
    $<TARGET_OBJECTS:embtable_share_map_store>
    $<TARGET_OBJECTS:embtable_emb_table>
    $<TARGET_OBJECTS:embtable_emb_table_client>
)
target_include_directories(mooncake_embtable_store PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${boomphf_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include/cachelib_memory_allocator/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include/cachelib_memory_allocator/fake_include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include/cachelib_memory_allocator
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-store/include/mooncake-store/proto
    ${CMAKE_CURRENT_BINARY_DIR}/../../mooncake-store/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include
)
target_link_libraries(mooncake_embtable_store PUBLIC
    mooncake_store glog::glog)

if(BUILD_SHARED_LIBS)
    install(TARGETS mooncake_embtable_store DESTINATION lib)
endif()

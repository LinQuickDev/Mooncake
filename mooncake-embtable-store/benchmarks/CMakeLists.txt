add_executable(embtable_cluster_bench embtable_cluster_bench.cpp)
target_include_directories(embtable_cluster_bench PRIVATE
    ${EMBTABLE_INCLUDE_DIR}
    ${MOONCAKE_STORE_INCLUDE_DIRS}
    ${MOONCAKE_TE_INCLUDE_DIR}
)
target_link_libraries(embtable_cluster_bench PRIVATE
    mooncake_embtable_store
    gflags::gflags
    glog::glog
    pthread
)

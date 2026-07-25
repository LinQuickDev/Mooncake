#!/bin/bash
# Script to build Mooncake RPM package for multiple platforms
# Usage: ./scripts/build_rpm.sh [BUILD_DIR] [OUTPUT_DIR] [PLATFORM]
# Example: ./scripts/build_rpm.sh build rpm-output x86_64
# Example: ./scripts/build_rpm.sh build rpm-output aarch64
# Example: ./scripts/build_rpm.sh build rpm-output all (build both platforms)

set -e  # Exit immediately if a command exits with a non-zero status
set -x

# Get build directory from environment variable or argument
BUILD_DIR="${BUILD_DIR:-${1:-build}}"
if [[ "${BUILD_DIR}" = /* ]]; then
    BUILD_DIR_ABS="${BUILD_DIR}"
else
    BUILD_DIR_ABS="$(pwd)/${BUILD_DIR}"
fi

# Get output directory from environment variable or argument
OUTPUT_DIR="${OUTPUT_DIR:-${2:-rpm-output}}"
if [[ "${OUTPUT_DIR}" != /* ]]; then
    OUTPUT_DIR="$(pwd)/${OUTPUT_DIR}"
fi

# Detect current host architecture
HOST_ARCH=$(uname -m)

# Get target platform from environment variable or argument
# If not specified, default to the current host architecture
# Supported: x86_64, aarch64, all (build both)
TARGET_PLATFORM="${TARGET_PLATFORM:-${3:-${HOST_ARCH}}}"

# Package information
PACKAGE_NAME="mooncake"
PACKAGE_VERSION="1.0.0"
PACKAGE_RELEASE="1"
PACKAGE_SUMMARY="Mooncake distributed KVCache store"
PACKAGE_DESCRIPTION="High-performance distributed KVCache store for LLM inference"
PACKAGE_VENDOR="KVCache.AI"
PACKAGE_LICENSE="Apache-2.0"

echo "Building RPM package for Mooncake"
echo "Build directory: ${BUILD_DIR_ABS}"
echo "Output directory: ${OUTPUT_DIR}"
echo "Target platform: ${TARGET_PLATFORM}"
echo "Host architecture: ${HOST_ARCH}"

# Ensure LD_LIBRARY_PATH includes build directories
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${BUILD_DIR_ABS}/mooncake-common:/usr/local/lib

# Clean previous build
echo "Cleaning previous RPM build..."
rm -rf rpmbuild/
rm -rf ${OUTPUT_DIR}/

fail_ubdiag_packaging() {
    echo "Error: UbDiag RPM provenance check failed: $*" >&2
    return 1
}

load_ubdiag_rpm_manifest() {
    local manifest=$1
    local key=""
    local value=""

    if [ ! -f "${manifest}" ]; then
        fail_ubdiag_packaging "manifest not found: ${manifest}"
        return 1
    fi

    while IFS='=' read -r key value || [ -n "${key}" ]; do
        value="${value%$'\r'}"
        case "${key}" in
            MOONCAKE_UBDIAG_LAYER|MOONCAKE_UBDIAG_GIT_REPOSITORY|\
            MOONCAKE_UBDIAG_GIT_TAG|MOONCAKE_UBDIAG_EXPECTED_COMMIT|\
            MOONCAKE_UBDIAG_RESOLVED_COMMIT|MOONCAKE_UBDIAG_SOURCE_DIR)
                printf -v "${key}" '%s' "${value}"
                ;;
            "")
                ;;
            *)
                fail_ubdiag_packaging \
                    "unknown key '${key}' in ${manifest}"
                return 1
                ;;
        esac
    done < "${manifest}"
}

verify_ubdiag_source_provenance() {
    local source_dir=""
    local current_commit=""
    local dirty=""
    local required_tool=""

    for required_tool in git readelf readlink sha256sum; do
        if ! command -v "${required_tool}" >/dev/null 2>&1; then
            fail_ubdiag_packaging \
                "required tool is unavailable: ${required_tool}"
            return 1
        fi
    done

    case "${MOONCAKE_UBDIAG_LAYER}" in
        mock|vendored)
            ;;
        *)
            fail_ubdiag_packaging \
                "unsupported layer '${MOONCAKE_UBDIAG_LAYER:-unset}'"
            return 1
            ;;
    esac

    if [[ ! "${MOONCAKE_UBDIAG_EXPECTED_COMMIT}" =~ ^[0-9a-fA-F]{40}$ ]]; then
        fail_ubdiag_packaging \
            "expected commit is not a full SHA: ${MOONCAKE_UBDIAG_EXPECTED_COMMIT}"
        return 1
    fi
    if [ "${MOONCAKE_UBDIAG_EXPECTED_COMMIT,,}" != \
         "${MOONCAKE_UBDIAG_RESOLVED_COMMIT,,}" ]; then
        fail_ubdiag_packaging \
            "configured commit ${MOONCAKE_UBDIAG_RESOLVED_COMMIT} does not match expected ${MOONCAKE_UBDIAG_EXPECTED_COMMIT}"
        return 1
    fi

    source_dir="$(readlink -f "${MOONCAKE_UBDIAG_SOURCE_DIR}" 2>/dev/null || true)"
    if [ -z "${source_dir}" ] || [ ! -f "${source_dir}/CMakeLists.txt" ]; then
        fail_ubdiag_packaging \
            "verified source directory is unavailable: ${MOONCAKE_UBDIAG_SOURCE_DIR}"
        return 1
    fi

    current_commit="$(
        git -C "${source_dir}" rev-parse --verify HEAD 2>/dev/null || true
    )"
    if [ "${current_commit,,}" != "${MOONCAKE_UBDIAG_EXPECTED_COMMIT,,}" ]; then
        fail_ubdiag_packaging \
            "source changed after CMake configure: expected ${MOONCAKE_UBDIAG_EXPECTED_COMMIT}, found ${current_commit}"
        return 1
    fi
    dirty="$(git -C "${source_dir}" status --porcelain --untracked-files=all)"
    if [ -n "${dirty}" ]; then
        fail_ubdiag_packaging \
            "source worktree changed after CMake configure: ${source_dir}"
        printf '%s\n' "${dirty}" >&2
        return 1
    fi

    MOONCAKE_UBDIAG_SOURCE_DIR="${source_dir}"
    MOONCAKE_UBDIAG_RESOLVED_COMMIT="${current_commit,,}"
}

# Function to build RPM for a specific platform
build_rpm_for_platform() {
    local PLATFORM=$1
    local LIB_DIR="lib64"
    local UBDIAG_RPM_FILES=""
    local MOONCAKE_UBDIAG_LAYER=""
    local MOONCAKE_UBDIAG_GIT_REPOSITORY=""
    local MOONCAKE_UBDIAG_GIT_TAG=""
    local MOONCAKE_UBDIAG_EXPECTED_COMMIT=""
    local MOONCAKE_UBDIAG_RESOLVED_COMMIT=""
    local MOONCAKE_UBDIAG_SOURCE_DIR=""
    
    echo "Building RPM for platform: ${PLATFORM}"
    
    # Determine lib directory based on platform
    if [ "${PLATFORM}" = "aarch64" ]; then
        LIB_DIR="lib64"  # ARM64 also uses lib64 on most distros
    elif [ "${PLATFORM}" = "x86_64" ]; then
        LIB_DIR="lib64"
    else
        echo "Error: Unsupported platform ${PLATFORM}"
        return 1
    fi
    
    # Create RPM build directory structure for this platform
    mkdir -p rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
    mkdir -p rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}
    
    # Create target directories in BUILDROOT
    mkdir -p rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/{usr/{bin,${LIB_DIR},include,share/doc/mooncake},etc/{mooncake,ubdiag}}
    
    # -------------------------------------------------------------------------
    # Copy executables
    # -------------------------------------------------------------------------
    echo "Copying executables..."
    
    # Determine build subdirectory based on platform
    local PLATFORM_BUILD_DIR="${BUILD_DIR_ABS}"
    if [ "${PLATFORM}" != "${HOST_ARCH}" ]; then
        # Cross-compilation path
        PLATFORM_BUILD_DIR="${BUILD_DIR_ABS}-${PLATFORM}"
        echo "Cross-compilation detected, looking in ${PLATFORM_BUILD_DIR}"
    fi

    local UBDIAG_RPM_MANIFEST="${PLATFORM_BUILD_DIR}/mooncake_ubdiag_rpm.env"
    echo "Reading verified UbDiag RPM manifest: ${UBDIAG_RPM_MANIFEST}"
    load_ubdiag_rpm_manifest "${UBDIAG_RPM_MANIFEST}"
    verify_ubdiag_source_provenance
    
    # mooncake_master
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-store/src/mooncake_master ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-store/src/mooncake_master rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/bin/
        chmod 755 rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/bin/mooncake_master
    else
        echo "Warning: mooncake_master not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # mooncake_client
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-store/src/mooncake_client ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-store/src/mooncake_client rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/bin/
        chmod 755 rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/bin/mooncake_client
    else
        echo "Warning: mooncake_client not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # stress_cluster_bench
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-store/benchmarks/stress_cluster_bench ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-store/benchmarks/stress_cluster_bench rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/bin/
        chmod 755 rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/bin/stress_cluster_bench
    else
        echo "Warning: stress_cluster_bench not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # transfer_engine_bench
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-transfer-engine/example/transfer_engine_bench ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-transfer-engine/example/transfer_engine_bench rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/bin/
        chmod 755 rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/bin/transfer_engine_bench
    else
        echo "Warning: transfer_engine_bench not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # -------------------------------------------------------------------------
    # Copy libraries
    # -------------------------------------------------------------------------
    echo "Copying shared libraries..."
    
    # libmooncake_store.so
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-store/src/libmooncake_store.so ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-store/src/libmooncake_store.so rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/${LIB_DIR}/
    else
        echo "Warning: libmooncake_store.so not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # libtransfer_engine.so
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-transfer-engine/src/libtransfer_engine.so ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-transfer-engine/src/libtransfer_engine.so rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/${LIB_DIR}/
    else
        echo "Warning: libtransfer_engine.so not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # libasio.so
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-common/libasio.so ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-common/libasio.so rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/${LIB_DIR}/
    else
        echo "Warning: libasio.so not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # libetcd_wrapper.so
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-common/etcd/libetcd_wrapper.so ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-common/etcd/libetcd_wrapper.so rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/${LIB_DIR}/
    else
        echo "Warning: libetcd_wrapper.so not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # libmooncake_common.so
    if [ -f ${PLATFORM_BUILD_DIR}/mooncake-common/libmooncake_common.so ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-common/libmooncake_common.so rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/${LIB_DIR}/
    elif [ -f ${PLATFORM_BUILD_DIR}/mooncake-common/src/libmooncake_common.so ]; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-common/src/libmooncake_common.so rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/${LIB_DIR}/
    else
        echo "Warning: libmooncake_common.so not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # engine.so (Python binding)
    if compgen -G "${PLATFORM_BUILD_DIR}/mooncake-integration/engine.*.so" >/dev/null; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-integration/engine.*.so rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/${LIB_DIR}/libmooncake_engine.so
    else
        echo "Warning: engine.so not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi
    
    # store.so (Python binding)
    if compgen -G "${PLATFORM_BUILD_DIR}/mooncake-integration/store.*.so" >/dev/null; then
        cp ${PLATFORM_BUILD_DIR}/mooncake-integration/store.*.so rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/${LIB_DIR}/libmooncake_store_python.so
    else
        echo "Warning: store.so not found in ${PLATFORM_BUILD_DIR}, skipping..."
    fi

    # -------------------------------------------------------------------------
    # Copy the verified UbDiag runtime selected by Mooncake
    # -------------------------------------------------------------------------
    local BUILDROOT="rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}"
    if [ "${MOONCAKE_UBDIAG_LAYER}" = "vendored" ]; then
        local ubdiag_binary_dir
        local ubdiag_cli
        local ubdiag_library_dir
        local ubdiag_config
        local ubdiag_library_real
        local staged_library_real
        local build_source_commit
        local ubdiag_cli_sha256=""
        local ubdiag_library_sha256=""

        ubdiag_binary_dir="$(
            readlink -f "${PLATFORM_BUILD_DIR}/_deps/ubdiag-build" 2>/dev/null ||
                true
        )"
        ubdiag_cli="${ubdiag_binary_dir}/src/cli/ubdiag"
        ubdiag_library_dir="${ubdiag_binary_dir}/src/sdk"
        ubdiag_config="${MOONCAKE_UBDIAG_SOURCE_DIR}/config/ubdiag.conf.example"
        build_source_commit="$(
            tr -d '[:space:]' \
                < "${ubdiag_binary_dir}/mooncake-source-commit.txt" \
                2>/dev/null || true
        )"

        if [ -z "${ubdiag_binary_dir}" ] ||
           [ "${build_source_commit}" != "${MOONCAKE_UBDIAG_RESOLVED_COMMIT}" ]; then
            fail_ubdiag_packaging \
                "current _deps/ubdiag-build has no matching source-commit marker"
            return 1
        fi
        if [ ! -x "${ubdiag_cli}" ] ||
           [ ! -f "${ubdiag_config}" ] ||
           ! compgen -G "${ubdiag_library_dir}/libubdiag.so*" >/dev/null ||
           ! readelf -d "${ubdiag_cli}" 2>/dev/null |
             grep -q 'Shared library:.*libubdiag\.so'; then
            fail_ubdiag_packaging \
                "Mooncake-built CLI, library, or config is missing/inconsistent"
            return 1
        fi
        ubdiag_library_real="$(readlink -f "${ubdiag_library_dir}/libubdiag.so")"
        case "${ubdiag_library_real}" in
            "${ubdiag_library_dir}"/*)
                ;;
            *)
                fail_ubdiag_packaging "libubdiag.so escapes its build directory"
                return 1
                ;;
        esac

        cp "${ubdiag_cli}" "${BUILDROOT}/usr/bin/ubdiag"
        chmod 755 "${BUILDROOT}/usr/bin/ubdiag"
        cp -a "${ubdiag_library_dir}"/libubdiag.so* \
            "${BUILDROOT}/usr/${LIB_DIR}/"
        cp "${ubdiag_config}" "${BUILDROOT}/etc/ubdiag/ubdiag.conf"

        cmp -s "${ubdiag_cli}" "${BUILDROOT}/usr/bin/ubdiag" ||
            {
                fail_ubdiag_packaging "staged CLI differs from build output"
                return 1
            }
        staged_library_real="$(
            readlink -f "${BUILDROOT}/usr/${LIB_DIR}/libubdiag.so"
        )"
        cmp -s "${ubdiag_library_real}" "${staged_library_real}" ||
            {
                fail_ubdiag_packaging "staged library differs from build output"
                return 1
            }
        ubdiag_cli_sha256="$(sha256sum "${ubdiag_cli}" | awk '{print $1}')"
        ubdiag_library_sha256="$(sha256sum "${ubdiag_library_real}" | awk '{print $1}')"
        cat > "${BUILDROOT}/usr/share/doc/mooncake/ubdiag-provenance.txt" << EOF
layer=vendored
repository=${MOONCAKE_UBDIAG_GIT_REPOSITORY}
tag=${MOONCAKE_UBDIAG_GIT_TAG}
commit=${MOONCAKE_UBDIAG_RESOLVED_COMMIT}
cli_sha256=${ubdiag_cli_sha256}
library_file=$(basename "${ubdiag_library_real}")
library_sha256=${ubdiag_library_sha256}
EOF
        UBDIAG_RPM_FILES="
/usr/bin/ubdiag
/usr/${LIB_DIR}/libubdiag.so*
%config(noreplace) /etc/ubdiag/ubdiag.conf
/usr/share/doc/mooncake/ubdiag-provenance.txt"
        echo "UbDiag RPM source verified: ${MOONCAKE_UBDIAG_RESOLVED_COMMIT}"
    else
        if compgen -G "${BUILDROOT}/usr/${LIB_DIR}/libubdiag.so*" >/dev/null ||
           [ -e "${BUILDROOT}/usr/bin/ubdiag" ]; then
            fail_ubdiag_packaging \
                "mock layer must not stage any UbDiag CLI or shared library"
            return 1
        fi
    fi
    
    # -------------------------------------------------------------------------
    # Copy header files (only core headers for real_client and dummy_client)
    # -------------------------------------------------------------------------
    echo "Copying core header files for real_client and dummy_client..."
    
    # Create include directories
    mkdir -p rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/include/mooncake
    
    # Core client headers (real_client and dummy_client dependencies)
    # These are the essential headers needed for client-side development
    CORE_HEADERS=(
        # Main client headers
        "real_client.h"
        "dummy_client.h"
        "pyclient.h"
        
        # pyclient dependencies
        "client_service.h"
        "client_buffer.hpp"
        "mutex.h"
        "utils.h"
        "file_storage.h"
        
        # real_client dependencies
        "rpc_types.h"
        
        # dummy_client dependencies
        "shm_helper.h"
        "client_metric.h"
        
        # Common types and utilities
        "types.h"
        "segment.h"
        "replica.h"
        "rpc_helper.h"
        "rpc_service.h"
        "config_helper.h"
        "allocator.h"
        "allocation_strategy.h"
        "client_buffer.hpp"
        "aligned_client_buffer.hpp"
        "eviction_strategy.h"
        "metadata_store.h"
        "mmap_arena.h"
        "pinned_buffer_pool.h"
        
        # C API headers
        "store_c.h"
    )
    
    # Copy core store headers
    for header in "${CORE_HEADERS[@]}"; do
        if [ -f mooncake-store/include/${header} ]; then
            cp mooncake-store/include/${header} rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/include/mooncake/
        else
            echo "Warning: Core header ${header} not found"
        fi
    done
    
    # Transfer engine core headers (needed by real_client)
    TRANSFER_ENGINE_HEADERS=(
        "transfer_engine_c.h"
        "transfer_engine.h"
        "common.h"
        "config.h"
        "error.h"
        "memory_location.h"
        "multi_transport.h"
        "topology.h"
    )
    
    # Copy transfer engine headers
    for header in "${TRANSFER_ENGINE_HEADERS[@]}"; do
        if [ -f mooncake-transfer-engine/include/${header} ]; then
            cp mooncake-transfer-engine/include/${header} rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/usr/include/mooncake/
        else
            echo "Warning: Transfer engine header ${header} not found"
        fi
    done
    
    # Note: Excluding the following directories as they are not needed for basic client usage:
    # - cachelib_memory_allocator/ (memory allocator internals)
    # - engram/ (engram store specific)
    # - ha/ (high availability - leader coordinator, oplog, snapshot)
    # - hf3fs/ (HF3 filesystem)
    # - offset_allocator/ (offset allocation)
    # - serialize/ (serialization utilities)
    # - spdk/ (SPDK integration)
    # - utils/s3_helper.h, zstd_util.h (specific utilities)
    
    # -------------------------------------------------------------------------
    # Copy configuration files
    # -------------------------------------------------------------------------
    echo "Copying configuration files..."
    
    # Master configuration
    if [ -f mooncake-store/conf/master.yaml ]; then
        cp mooncake-store/conf/master.yaml rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/etc/mooncake/
    fi
    
    if [ -f mooncake-store/conf/master.json ]; then
        cp mooncake-store/conf/master.json rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}/etc/mooncake/
    fi

    local staged_elf=""
    local staged_ubdiag_dependency_count=0
    while IFS= read -r -d '' staged_elf; do
        case "${staged_elf}" in
            */usr/bin/ubdiag|*/usr/${LIB_DIR}/libubdiag.so*)
                continue
                ;;
        esac
        if readelf -d "${staged_elf}" 2>/dev/null |
           grep -q 'Shared library:.*libubdiag\.so'; then
            staged_ubdiag_dependency_count=$((staged_ubdiag_dependency_count + 1))
        fi
    done < <(find "${BUILDROOT}" -type f -print0)
    if [ "${MOONCAKE_UBDIAG_LAYER}" = "mock" ] &&
       [ "${staged_ubdiag_dependency_count}" -ne 0 ]; then
        fail_ubdiag_packaging \
            "mock RPM contains ${staged_ubdiag_dependency_count} ELF file(s) linked to libubdiag"
        return 1
    fi
    if [ "${MOONCAKE_UBDIAG_LAYER}" = "vendored" ] &&
       [ "${staged_ubdiag_dependency_count}" -eq 0 ]; then
        fail_ubdiag_packaging \
            "vendored RPM has no ELF consumer linked to the packaged libubdiag"
        return 1
    fi

    local OPTIONAL_RPM_FILES=""
    local optional_rpm_file=""
    for optional_rpm_file in \
        /usr/bin/transfer_engine_bench \
        /usr/${LIB_DIR}/libmooncake_store.so \
        /usr/${LIB_DIR}/libtransfer_engine.so \
        /usr/${LIB_DIR}/libmooncake_common.so \
        /usr/${LIB_DIR}/libetcd_wrapper.so \
        /usr/${LIB_DIR}/libmooncake_engine.so \
        /usr/${LIB_DIR}/libmooncake_store_python.so; do
        if [ -e "${BUILDROOT}${optional_rpm_file}" ]; then
            OPTIONAL_RPM_FILES+="${optional_rpm_file}"$'\n'
        fi
    done
    
    # -------------------------------------------------------------------------
    # Create RPM spec file
    # -------------------------------------------------------------------------
    echo "Creating RPM spec file for ${PLATFORM}..."
    
    cat > rpmbuild/SPECS/${PACKAGE_NAME}-${PLATFORM}.spec << EOF
Name:           ${PACKAGE_NAME}
Version:        ${PACKAGE_VERSION}
Release:        ${PACKAGE_RELEASE}%{?dist}
Summary:        ${PACKAGE_SUMMARY}
License:        ${PACKAGE_LICENSE}
Vendor:         ${PACKAGE_VENDOR}
URL:            https://github.com/KVCache-AI/Mooncake
BuildArch:      ${PLATFORM}

# System dependencies - these will be automatically resolved by RPM during installation
Requires:       libgflags.so.2.2()(64bit)
Requires:       libglog.so.1()(64bit)
Requires:       libjsoncpp.so.25()(64bit)
Requires:       libxxhash.so.0()(64bit)
Requires:       libyaml-cpp.so.0.7()(64bit)
Requires:       liburing.so.2()(64bit)

# Optional dependencies - RDMA support (required for network transport)
Requires:       libibverbs.so.1()(64bit)

# Optional dependencies - CUDA support (for GPU acceleration, not required for basic functionality)
# These are recommended but not required for basic operations
Recommends:     libcudart.so.12()(64bit)
Recommends:     liburma.so.0()(64bit)

# Optional dependencies - not required but recommended
Requires(pre):  /usr/sbin/ldconfig

%description
${PACKAGE_DESCRIPTION}

%files
/usr/bin/mooncake_master
/usr/bin/mooncake_client
/usr/bin/stress_cluster_bench
/usr/${LIB_DIR}/libasio.so
${OPTIONAL_RPM_FILES}
${UBDIAG_RPM_FILES}
/usr/include/mooncake/*.h
/usr/include/mooncake/*.hpp
/etc/mooncake/master.yaml
/etc/mooncake/master.json

%post
/sbin/ldconfig

%postun
/sbin/ldconfig

%changelog
* $(date +"%a %b %d %Y") KVCache.AI <support@kvcache.ai> - ${PACKAGE_VERSION}-${PACKAGE_RELEASE}
- Initial RPM package for ${PLATFORM}
EOF
    
    # -------------------------------------------------------------------------
    # Build RPM package
    # -------------------------------------------------------------------------
    echo "Building RPM package for ${PLATFORM}..."
    
    # Ensure rpmbuild is available
    if ! command -v rpmbuild &>/dev/null; then
        echo "Error: rpmbuild not found. Please install rpm-build package."
        exit 1
    fi
    
    # Create platform-specific output directory
    mkdir -p ${OUTPUT_DIR}/${PLATFORM}
    
    # Build the RPM
    rpmbuild -bb \
        --define "_topdir $(pwd)/rpmbuild" \
        --define "_rpmdir ${OUTPUT_DIR}" \
        rpmbuild/SPECS/${PACKAGE_NAME}-${PLATFORM}.spec
    
    # Move RPM to platform-specific directory
    mv ${OUTPUT_DIR}/${PLATFORM}/*.rpm ${OUTPUT_DIR}/ 2>/dev/null || true
    
    # Cleanup BUILDROOT for next platform
    rm -rf rpmbuild/BUILDROOT/
}

# -----------------------------------------------------------------------------
# Main build logic
# -----------------------------------------------------------------------------
if [ "${TARGET_PLATFORM}" = "all" ]; then
    echo "Building RPM packages for all supported platforms..."
    
    # Build for x86_64
    build_rpm_for_platform "x86_64"
    
    # Build for aarch64 (if cross-compilation is available or running on ARM)
    if [ "${HOST_ARCH}" = "aarch64" ] || [ -d "${BUILD_DIR}-aarch64" ]; then
        build_rpm_for_platform "aarch64"
    else
        echo "Warning: Skipping aarch64 build - no cross-compilation build directory found"
    fi
    
elif [ "${TARGET_PLATFORM}" = "x86_64" ] || [ "${TARGET_PLATFORM}" = "aarch64" ]; then
    build_rpm_for_platform "${TARGET_PLATFORM}"
    
else
    echo "Error: Unsupported platform ${TARGET_PLATFORM}"
    echo "Supported platforms: x86_64, aarch64, all"
    exit 1
fi

# List created RPM files
echo "RPM packages built successfully!"
echo "Created RPM files:"
ls -la ${OUTPUT_DIR}/*.rpm 2>/dev/null || echo "No RPM files created"

# Cleanup
rm -rf rpmbuild/

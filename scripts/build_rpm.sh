#!/bin/bash
# Script to build Mooncake RPM package for multiple platforms
# Usage: ./scripts/build_rpm.sh [BUILD_DIR] [OUTPUT_DIR] [PLATFORM]
# Example: ./scripts/build_rpm.sh build rpm-output x86_64
# Example: ./scripts/build_rpm.sh build rpm-output aarch64
# Example: ./scripts/build_rpm.sh build rpm-output all (build both platforms)

set -euo pipefail
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOONCAKE_SOURCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

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
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:${BUILD_DIR_ABS}/mooncake-common:/usr/local/lib"

# Clean previous build
echo "Cleaning previous RPM build..."
rm -rf rpmbuild/
rm -rf "${OUTPUT_DIR}/"

fail_ubdiag_packaging() {
    echo "Error: UbDiag RPM packaging check failed: $*" >&2
    return 1
}

remove_staged_rpath() {
    local staged_elf="$1"

    if readelf -d "${staged_elf}" 2>/dev/null |
       grep -Eq '\((RPATH|RUNPATH)\)'; then
        cmake -DINPUT_FILE="${staged_elf}" \
            -P "${MOONCAKE_SOURCE_DIR}/cmake/RemoveRpath.cmake"
    fi
    if readelf -d "${staged_elf}" 2>/dev/null |
       grep -Eq '\((RPATH|RUNPATH)\)'; then
        fail_ubdiag_packaging \
            "staged ELF still contains RPATH/RUNPATH: ${staged_elf}"
        return 1
    fi
}

load_ubdiag_manifest() {
    local manifest="$1"
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
            MOONCAKE_UBDIAG_RESOLVED_COMMIT|MOONCAKE_UBDIAG_SOURCE_DIR|\
            MOONCAKE_UBDIAG_PACKAGE_VERSION|MOONCAKE_UBDIAG_PACKAGE_DIR|\
            MOONCAKE_UBDIAG_SYSTEM_PREFIX|MOONCAKE_UBDIAG_SYSTEM_LIBRARY|\
            MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM|MOONCAKE_UBDIAG_SYSTEM_CLI|\
            MOONCAKE_UBDIAG_SYSTEM_CLI_RPM|\
            MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH|\
            MOONCAKE_UBDIAG_SYSTEM_CONFIG)
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

query_rpm_file_owner() {
    local file_path="$1"
    rpm -qf --qf '%{NAME}|%{VERSION}-%{RELEASE}.%{ARCH}' "${file_path}"
}

verify_system_ubdiag_inputs() {
    local library_record=""
    local cli_record=""
    local library_owner=""
    local cli_owner=""
    local library_identity=""
    local cli_identity=""

    for required_tool in cmake readelf readlink rpm; do
        if ! command -v "${required_tool}" >/dev/null 2>&1; then
            fail_ubdiag_packaging \
                "required tool is unavailable: ${required_tool}"
            return 1
        fi
    done
    if [ ! -f "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" ] ||
       [ ! -x "${MOONCAKE_UBDIAG_SYSTEM_CLI}" ]; then
        fail_ubdiag_packaging \
            "configured system UbDiag library or CLI is unavailable"
        return 1
    fi

    MOONCAKE_UBDIAG_SYSTEM_LIBRARY="$(
        readlink -f "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}"
    )"
    MOONCAKE_UBDIAG_SYSTEM_CLI="$(
        readlink -f "${MOONCAKE_UBDIAG_SYSTEM_CLI}"
    )"
    case "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" in
        "${MOONCAKE_UBDIAG_SYSTEM_PREFIX}"/lib/libubdiag.so*|\
        "${MOONCAKE_UBDIAG_SYSTEM_PREFIX}"/lib64/libubdiag.so*)
            ;;
        *)
            fail_ubdiag_packaging \
                "libubdiag.so is outside the configured system prefix"
            return 1
            ;;
    esac
    case "${MOONCAKE_UBDIAG_SYSTEM_CLI}" in
        "${MOONCAKE_UBDIAG_SYSTEM_PREFIX}"/bin/ubdiag)
            ;;
        *)
            fail_ubdiag_packaging \
                "ubdiag CLI is outside the configured system prefix"
            return 1
            ;;
    esac

    library_record="$(query_rpm_file_owner \
        "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}")"
    cli_record="$(query_rpm_file_owner "${MOONCAKE_UBDIAG_SYSTEM_CLI}")"
    IFS='|' read -r library_owner library_identity <<< "${library_record}"
    IFS='|' read -r cli_owner cli_identity <<< "${cli_record}"
    if [[ "${library_owner,,}" != *ubdiag* ]] ||
       [[ "${cli_owner,,}" != *ubdiag* ]]; then
        fail_ubdiag_packaging \
            "library or CLI is not owned by an UbDiag RPM"
        return 1
    fi
    if [ "${library_identity}" != "${cli_identity}" ] ||
       [ "${library_identity}" != \
         "${MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH}" ] ||
       [ "${library_owner}" != "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM}" ] ||
       [ "${cli_owner}" != "${MOONCAKE_UBDIAG_SYSTEM_CLI_RPM}" ]; then
        fail_ubdiag_packaging \
            "system UbDiag RPM changed after CMake configure: library=${library_record}, CLI=${cli_record}, configured=${MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH}"
        return 1
    fi
}

# Function to build RPM for a specific platform
build_rpm_for_platform() {
    local PLATFORM=$1
    local LIB_DIR="lib64"
    local BUILDROOT=""
    local UBDIAG_CONFIG_RPM_FILE=""
    local MOONCAKE_UBDIAG_LAYER=""
    local MOONCAKE_UBDIAG_GIT_REPOSITORY=""
    local MOONCAKE_UBDIAG_GIT_TAG=""
    local MOONCAKE_UBDIAG_EXPECTED_COMMIT=""
    local MOONCAKE_UBDIAG_RESOLVED_COMMIT=""
    local MOONCAKE_UBDIAG_SOURCE_DIR=""
    local MOONCAKE_UBDIAG_PACKAGE_VERSION=""
    local MOONCAKE_UBDIAG_PACKAGE_DIR=""
    local MOONCAKE_UBDIAG_SYSTEM_PREFIX=""
    local MOONCAKE_UBDIAG_SYSTEM_LIBRARY=""
    local MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM=""
    local MOONCAKE_UBDIAG_SYSTEM_CLI=""
    local MOONCAKE_UBDIAG_SYSTEM_CLI_RPM=""
    local MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH=""
    local MOONCAKE_UBDIAG_SYSTEM_CONFIG=""

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
    BUILDROOT="rpmbuild/BUILDROOT/${PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}.${PLATFORM}"
    mkdir -p "${BUILDROOT}"

    # Create target directories in BUILDROOT
    mkdir -p "${BUILDROOT}/usr/bin" "${BUILDROOT}/usr/${LIB_DIR}" \
        "${BUILDROOT}/usr/include" "${BUILDROOT}/etc/mooncake"

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

    local UBDIAG_MANIFEST="${PLATFORM_BUILD_DIR}/mooncake_ubdiag.env"
    echo "Reading Mooncake UbDiag manifest: ${UBDIAG_MANIFEST}"
    load_ubdiag_manifest "${UBDIAG_MANIFEST}"
    case "${MOONCAKE_UBDIAG_LAYER}" in
        mock)
            ;;
        system)
            verify_system_ubdiag_inputs
            ;;
        *)
            fail_ubdiag_packaging \
                "unsupported layer '${MOONCAKE_UBDIAG_LAYER:-unset}'"
            return 1
            ;;
    esac

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
    # Stage the matching system UbDiag RPM payload for Layer 1
    # -------------------------------------------------------------------------
    if [ "${MOONCAKE_UBDIAG_LAYER}" = "system" ]; then
        local ubdiag_library_dir=""
        local ubdiag_candidate=""
        local staged_library_count=0

        ubdiag_library_dir="$(dirname "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}")"
        cp "${MOONCAKE_UBDIAG_SYSTEM_CLI}" "${BUILDROOT}/usr/bin/ubdiag"
        chmod 755 "${BUILDROOT}/usr/bin/ubdiag"

        while IFS= read -r -d '' ubdiag_candidate; do
            if [ "$(readlink -f "${ubdiag_candidate}")" = \
                 "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" ]; then
                cp -a "${ubdiag_candidate}" "${BUILDROOT}/usr/${LIB_DIR}/"
                staged_library_count=$((staged_library_count + 1))
            fi
        done < <(
            find "${ubdiag_library_dir}" -maxdepth 1 \
                -name 'libubdiag.so*' -print0
        )
        if [ "${staged_library_count}" -eq 0 ] ||
           [ ! -f "${BUILDROOT}/usr/${LIB_DIR}/$(basename \
                     "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}")" ]; then
            fail_ubdiag_packaging \
                "failed to stage the selected system libubdiag.so chain"
            return 1
        fi

        if [ -n "${MOONCAKE_UBDIAG_SYSTEM_CONFIG}" ]; then
            if [ ! -f "${MOONCAKE_UBDIAG_SYSTEM_CONFIG}" ]; then
                fail_ubdiag_packaging \
                    "configured UbDiag config disappeared: ${MOONCAKE_UBDIAG_SYSTEM_CONFIG}"
                return 1
            fi
            mkdir -p "${BUILDROOT}/etc/ubdiag"
            cp "${MOONCAKE_UBDIAG_SYSTEM_CONFIG}" \
                "${BUILDROOT}/etc/ubdiag/ubdiag.conf"
            UBDIAG_CONFIG_RPM_FILE="%config(noreplace) /etc/ubdiag/ubdiag.conf"
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

    # -------------------------------------------------------------------------
    # Validate the staged runtime before creating the RPM
    # -------------------------------------------------------------------------
    local staged_elf=""
    local staged_ubdiag_dependency_count=0
    for required_binary in mooncake_master mooncake_client; do
        if [ ! -x "${BUILDROOT}/usr/bin/${required_binary}" ]; then
            fail_ubdiag_packaging \
                "required Mooncake binary is missing: ${required_binary}"
            return 1
        fi
    done

    while IFS= read -r -d '' staged_elf; do
        if ! readelf -h "${staged_elf}" >/dev/null 2>&1; then
            continue
        fi
        remove_staged_rpath "${staged_elf}"
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

    if [ "${MOONCAKE_UBDIAG_LAYER}" = "mock" ]; then
        if [ "${staged_ubdiag_dependency_count}" -ne 0 ] ||
           [ -e "${BUILDROOT}/usr/bin/ubdiag" ] ||
           compgen -G "${BUILDROOT}/usr/${LIB_DIR}/libubdiag.so*" \
               >/dev/null; then
            fail_ubdiag_packaging \
                "mock RPM must not contain or depend on the UbDiag runtime"
            return 1
        fi
    else
        if [ "${staged_ubdiag_dependency_count}" -eq 0 ]; then
            fail_ubdiag_packaging \
                "system RPM has no Mooncake ELF linked to libubdiag.so"
            return 1
        fi
        if [ ! -x "${BUILDROOT}/usr/bin/ubdiag" ] ||
           ! compgen -G "${BUILDROOT}/usr/${LIB_DIR}/libubdiag.so*" \
               >/dev/null; then
            fail_ubdiag_packaging \
                "system RPM is missing the UbDiag CLI or shared library"
            return 1
        fi
    fi

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

# Refresh the dynamic linker cache after installation and removal.
Requires(post): /sbin/ldconfig
Requires(postun): /sbin/ldconfig

%description
${PACKAGE_DESCRIPTION}

%files
/usr/bin/*
/usr/${LIB_DIR}/*
/usr/include/mooncake/*
%config(noreplace) /etc/mooncake/*
${UBDIAG_CONFIG_RPM_FILE}

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

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
    mkdir -p "${OUTPUT_DIR}/${PLATFORM}"

    # Build the RPM
    rpmbuild -bb \
        --define "_topdir $(pwd)/rpmbuild" \
        --define "_rpmdir ${OUTPUT_DIR}" \
        rpmbuild/SPECS/${PACKAGE_NAME}-${PLATFORM}.spec

    # Move RPM to platform-specific directory
    mv "${OUTPUT_DIR}/${PLATFORM}"/*.rpm "${OUTPUT_DIR}/" 2>/dev/null || true

    local built_rpm=""
    local rpm_candidate=""
    for rpm_candidate in "${OUTPUT_DIR}"/*.rpm; do
        [ -e "${rpm_candidate}" ] || continue
        if [ "$(rpm -qp --qf '%{ARCH}' "${rpm_candidate}")" = \
             "${PLATFORM}" ]; then
            built_rpm="${rpm_candidate}"
        fi
    done
    if [ -z "${built_rpm}" ]; then
        fail_ubdiag_packaging \
            "rpmbuild did not produce a ${PLATFORM} Mooncake RPM"
        return 1
    fi

    local rpm_file_list=""
    rpm_file_list="$(rpm -qlp "${built_rpm}")"
    if [ "${MOONCAKE_UBDIAG_LAYER}" = "mock" ]; then
        if grep -Eq '^/usr/bin/ubdiag$|^/usr/lib64/libubdiag\.so' \
           <<< "${rpm_file_list}"; then
            fail_ubdiag_packaging \
                "mock RPM unexpectedly contains the UbDiag runtime"
            return 1
        fi
    else
        grep -qx '/usr/bin/ubdiag' <<< "${rpm_file_list}" ||
            {
                fail_ubdiag_packaging \
                    "system RPM does not contain /usr/bin/ubdiag"
                return 1
            }
        grep -Eq '^/usr/lib64/libubdiag\.so' <<< "${rpm_file_list}" ||
            {
                fail_ubdiag_packaging \
                    "system RPM does not contain libubdiag.so"
                return 1
            }
    fi

    # Install the payload into an isolated RPM root. This verifies that the
    # generated package can be installed without mutating the build machine.
    local install_root
    install_root="$(pwd)/rpmbuild/INSTALLROOT-${PLATFORM}"
    mkdir -p "${install_root}"
    rpm --root "${install_root}" --initdb
    rpm --root "${install_root}" -ivh --nodeps --noscripts "${built_rpm}"
    test -x "${install_root}/usr/bin/mooncake_master"
    test -x "${install_root}/usr/bin/mooncake_client"
    if [ "${MOONCAKE_UBDIAG_LAYER}" = "system" ]; then
        test -x "${install_root}/usr/bin/ubdiag"
        compgen -G "${install_root}/usr/${LIB_DIR}/libubdiag.so*" \
            >/dev/null
    fi
    rm -rf "${install_root}"

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
ls -la "${OUTPUT_DIR}"/*.rpm 2>/dev/null || echo "No RPM files created"

# Cleanup
rm -rf rpmbuild/

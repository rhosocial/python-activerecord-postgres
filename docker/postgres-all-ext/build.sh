#!/bin/bash
# docker/postgres-all-ext/build.sh
#
# Build PostgreSQL images with all extensions for multiple versions
#
# Usage:
#   ./build.sh [version]
#   ./build.sh all
#   ./build.sh 14 15 16
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR"

# Default versions
VERSIONS=${1:-"14 15 16 17 18"}

# Build for all versions
build_all() {
    echo "Building PostgreSQL images for versions: $VERSIONS"
    for ver in $VERSIONS; do
        echo "Building postgres-all-ext:$ver ..."
        docker build \
            --build-arg PG_VERSION=$ver \
            --build-arg POSTGIS_VERSION=3.4 \
            -t postgres-all-ext:$ver \
            -f Dockerfile \
            .
        echo "Built postgres-all-ext:$ver successfully"
    done
}

# Main
case "$1" in
    all)
        VERSIONS="14 15 16 17 18"
        build_all
        ;;
    help|--help|-h)
        echo "Usage: $0 [versions|all]"
        echo "  $0           # Build default versions (14 15 16 17 18)"
        echo "  $0 all       # Build all versions"
        echo "  $0 16       # Build specific version"
        echo "  $0 14 15 16 # Build multiple versions"
        ;;
    *)
        if [ $# -eq 0 ]; then
            VERSIONS="14 15 16 17 18"
        fi
        build_all
        ;;
esac

echo "Done!"
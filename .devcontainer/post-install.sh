#!/bin/bash
set -ex

# Convenience workspace directory for later use
WORKSPACE_DIR=$(pwd)

cd ${WORKSPACE_DIR}/app && npm install

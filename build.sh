#!/bin/bash
#
# Copyright © 2016-2025 The Thingsboard Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e # exit on any error

PROJECTS="msa/k8s-dns"

echo "Building and pushing [amd64,arm64] projects '$PROJECTS' ..."
echo "HELP: usage ./build.sh"
java -version

mvn license:format clean install -DskipTests

## Build and push AMD and ARM docker images using docker buildx
## Reference to article how to setup docker miltiplatform build environment: https://medium.com/@artur.klauser/building-multi-architecture-docker-images-with-buildx-27d80f7e2408
## install docker-ce from docker repo https://docs.docker.com/engine/install/ubuntu/
sudo apt install -y qemu-user-static binfmt-support
export DOCKER_CLI_EXPERIMENTAL=enabled
docker version
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

if docker buildx inspect mybuilder >/dev/null 2>&1; then
    echo "Using existing builder instance 'mybuilder'."
    docker buildx use mybuilder
else
    echo "Creating new builder instance 'mybuilder'."
    docker buildx create --name mybuilder
    docker buildx use mybuilder
    docker buildx inspect --bootstrap
fi

# build/push multi-platform Docker images
cd msa
mvn clean install -P push-docker-amd-arm-images
#
# Copyright 2019 is-land
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

FROM ghcr.io/chia7712/ohara/deps
# The root is used in ci image since dependencies from parent image (owned by root) are required in running QA.
# Coping dependencies from parent image is not solution as the chmod in docker is vary slow (see https://github.com/docker/for-linux/issues/388).
# Also, We are too poor to buy the super network which can download everything in running QA
WORKDIR /ohara

RUN git pull
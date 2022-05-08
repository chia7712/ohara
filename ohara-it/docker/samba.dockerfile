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

FROM centos:7.7.1908

RUN yum install -y \
  samba \
  samba-client \
  samba-common

COPY samba.sh /usr/sbin
RUN chmod +x /usr/sbin/samba.sh

# copy Tini
COPY --from=oharastream/ohara:deps /tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--", "samba.sh"]

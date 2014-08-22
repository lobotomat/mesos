# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

# Add 'subdir-objects' to build objects in respective subdirectories
# which is necessary for dealing with naming conflicts, i.e,,
# master/http.cpp and slave/http.cpp.
AUTOMAKE_OPTIONS = subdir-objects

# Initialize variables here so we can use += operator everywhere else.
lib_LTLIBRARIES =
noinst_LTLIBRARIES =
sbin_PROGRAMS =
bin_PROGRAMS =
pkglibexec_PROGRAMS =
dist_bin_SCRIPTS =
dist_pkglibexec_SCRIPTS =
nobase_dist_pkgdata_DATA =
nodist_sbin_SCRIPTS =
check_PROGRAMS =
dist_check_SCRIPTS =
check_SCRIPTS =
BUILT_SOURCES =
CLEANFILES =
EXTRA_DIST =
PHONY_TARGETS =
PNG_FILES =

# Sequence diagram rendering.
if HAS_SEQDIAG

SEGDIAG_PARAMS =							\
	"--size=768x4096"						\
	"--antialias"							\
	"-f $(top_srcdir)/docs/assets/fonts/generic/generic-Bold.otf"

PNG_FILES +=								\
	$(top_srcdir)/docs/images/ec_launch_seqdiag.png			\
	$(top_srcdir)/docs/images/ec_lifecycle_seqdiag.png		\
	$(top_srcdir)/docs/images/ec_kill_seqdiag.png 			\
	$(top_srcdir)/docs/images/ec_recover_seqdiag.png		\
	$(top_srcdir)/docs/images/ec_orphan_seqdiag.png

$(top_srcdir)/docs/images/%.png: $(top_srcdir)/docs/assets/%.diag
	$(SEQDIAG) $^ $(SEQDIAG_PARAMS) -o $@

BUILT_SOURCES += $(PNG_FILES)
CLEANFILES += $(PNG_FILES)

endif # HAS_SEQDIAG.

docs:	$(PNG_FILES)

.PHONY: $(PHONY_TARGETS)

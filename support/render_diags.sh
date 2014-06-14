#!/usr/bin/env bash

PYTHON_HOME="/usr/local/share/python"

SEGDIAG_PARAMS="--size=768x4096 --antialias"

echo rendering external containerizer launch sequence diagram ...
${PYTHON_HOME}/seqdiag							\
	"$(dirname ${0})/../docs/assets/ec_launch_seqdiag.diag"		\
	-f "$(dirname ${0})/../docs/assets/fonts/generic/generic-Bold.otf"	\
	${SEGDIAG_PARAMS} 						\
	-o "$(dirname ${0})/../docs/images/ec-launch-seqdiag.png"

echo rendering external containerizer isolation sequence diagram ...
${PYTHON_HOME}/seqdiag							\
	"$(dirname ${0})/../docs/assets/ec_lifecycle_seqdiag.diag"		\
	-f "$(dirname ${0})/../docs/assets/fonts/generic/generic-Bold.otf"	\
	${SEGDIAG_PARAMS} 						\
	-o "$(dirname ${0})/../docs/images/ec-lifecycle-seqdiag.png"

echo rendering external containerizer limitation sequence diagram ...
${PYTHON_HOME}/seqdiag							\
	"$(dirname ${0})/../docs/assets/ec_kill_seqdiag.diag"		\
	-f "$(dirname ${0})/../docs/assets/fonts/generic/generic-Bold.otf"	\
	${SEGDIAG_PARAMS} 						\
	-o "$(dirname ${0})/../docs/images/ec-kill-seqdiag.png"

echo rendering external containerizer recover sequence diagram ...
${PYTHON_HOME}/seqdiag							\
	"$(dirname ${0})/../docs/assets/ec_recover_seqdiag.diag"	\
	-f "$(dirname ${0})/../docs/assets/fonts/generic/generic-Bold.otf"	\
	${SEGDIAG_PARAMS} 						\
	-o "$(dirname ${0})/../docs/images/ec-recover-seqdiag.png"

echo rendering external containerizer orphan sequence diagram ...
${PYTHON_HOME}/seqdiag							\
	"$(dirname ${0})/../docs/assets/ec_orphan_seqdiag.diag"	\
	-f "$(dirname ${0})/../docs/assets/fonts/generic/generic-Bold.otf"	\
	${SEGDIAG_PARAMS} 						\
	-o "$(dirname ${0})/../docs/images/ec-orphan-seqdiag.png"

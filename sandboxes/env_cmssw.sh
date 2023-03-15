#!/usr/bin/env bash

action() {
    local origin="$( pwd )"

    export SCRAM_ARCH="$( law config hltp_config.scram_arch )"
    export CMSSW_VERSION="$( law config hltp_config.cmssw_version )"

    source /cvmfs/cms.cern.ch/cmsset_default.sh ""

    if [ ! -d "${HLTP_CMSSW}/${SCRAM_ARCH}/${CMSSW_VERSION}" ]; then
        mkdir -p "${HLTP_CMSSW}/${SCRAM_ARCH}"
        cd "${HLTP_CMSSW}/${SCRAM_ARCH}"
        scramv1 project CMSSW "${CMSSW_VERSION}" || return "$?"
        cd "${CMSSW_VERSION}/src"
        eval "$( scramv1 runtime -sh )" || return "$?"
        git cms-addpkg HLTrigger/Configuration
        scram b || return "$?"
    else
        cd "${HLTP_CMSSW}/${SCRAM_ARCH}/${CMSSW_VERSION}/src" || return "$?"
        eval "$( scramv1 runtime -sh )" || return "$?"
    fi

    cd "${origin}"
}
action "$@"

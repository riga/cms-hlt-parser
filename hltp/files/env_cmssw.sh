#!/usr/bin/env bash

action() {
    export SCRAM_ARCH="slc7_amd64_gcc700"
    export CMSSW_VERSION="CMSSW_10_2_16_UL"

    source /cvmfs/cms.cern.ch/cmsset_default.sh ""

    if [ ! -d "$HLTP_CMSSW/$SCRAM_ARCH/$CMSSW_VERSION" ]; then
        (\
            mkdir -p "$HLTP_CMSSW/$SCRAM_ARCH" &&
            cd "$HLTP_CMSSW/$SCRAM_ARCH" &&
            scramv1 project CMSSW "$CMSSW_VERSION" &&
            cd "$CMSSW_VERSION/src" &&
            eval `scramv1 runtime -sh` &&
            scram b
        )
    else
        (\
            cd "$HLTP_CMSSW/$SCRAM_ARCH/$CMSSW_VERSION/src" &&
            eval `scramv1 runtime -sh`
        )
    fi
}
action "$@"

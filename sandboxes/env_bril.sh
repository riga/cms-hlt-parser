#!/usr/bin/env bash

action() {
    # activate the python 3 conda env
    source "/cvmfs/cms-bril.cern.ch/brilconda3/bin/activate" ""

    # adjust local software paths
    local bril_dir="${HLTP_SOFTWARE}/bril"
    local pyv="$( python -c "import sys; print('{0.major}.{0.minor}'.format(sys.version_info))" )"
    export PYTHONPATH="${bril_dir}/lib/python${pyv}/site-packages:/cvmfs/cms-bril.cern.ch/brilconda3/lib/python${pyv}/site-packages:${PYTHONPATH}"
    export PATH="${bril_dir}/bin:${PATH}"

    if ! type brilcalc &> /dev/null; then
        PYTHONUSERBASE="${bril_dir}" pip install --user brilws
    fi
}
action "$@"

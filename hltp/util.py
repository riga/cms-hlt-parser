# coding: utf-8

"""
Helpful utilities.
"""


__all__ = ["dataset_is_mc", "pset_to_dict", "fwlite_loop", "text_to_process"]


import sys
import re

import law


def dataset_is_mc(dataset):
    r"""
    Returns *True* when a *dataset* is identified as being MC, and *False* otherwise. This function
    exploits the beginning of the campaign name which is always "Run\d{4}" for data, while it is
    """
    campaign = dataset.split("/")[2]
    is_data = bool(re.match(r"^Run\d{4}", campaign))
    return not is_data


def pset_to_dict(pset):
    """
    Converts a cms parameter set *pset* recursively to a python dictionary.
    """
    from FWCore.ParameterSet.Types import PSet

    return {
        key: (pset_to_dict(value) if isinstance(value, PSet) else value.value())
        for key, value in pset.parameters_().items()
    }


def fwlite_loop(path, handle_data=None, start=0, end=-1, object_type="Event"):
    """
    Opens one or more ROOT files defined by *path* and yields the FWLite event. When *handle_data*
    is not *None*, it is supposed to be a dictionary ``key -> {"type": ..., "label": ...}``. In that
    case, the handle products are yielded as well in a dictionary, mapped to the key, as
    ``(event, objects dict)``.
    """
    import ROOT
    ROOT.PyConfig.IgnoreCommandLineOptions = True
    ROOT.gROOT.SetBatch()

    ROOT.gSystem.Load("libFWCoreFWLite.so")
    ROOT.gSystem.Load("libDataFormatsFWLite.so")
    ROOT.FWLiteEnabler.enable()

    from DataFormats.FWLite import Events, Runs, Handle

    paths = path if isinstance(path, (list, tuple)) else [path]

    handles = {}
    if handle_data:
        for key, data in handle_data.items():
            handles[key] = Handle(data["type"])

    objects = locals()[object_type + "s"](paths)
    if start > 0:
        objects.to(start)

    for i, obj in enumerate(objects):
        if end >= 0 and (start + i) >= end:
            break

        if handle_data:
            products = {}
            for key, data in handle_data.items():
                obj.getByLabel(data["label"], handles[key])
                products[key] = handles[key].product()
            yield obj, products
        else:
            yield obj


def text_to_process(content, name="INTERACTIVE"):
    """
    Loads the *content* of a CMSSW Python config file from a string, creates a ``cms.Process`` named
    *name* and returns it. This function requires a CMSSW environment.
    """
    import FWCore.ParameterSet.Config as cms

    # create a tmp dir
    tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
    tmp_dir.touch()

    # dump the file content and make it importable
    tmp_dir.child("cfg.py", type="f").dump(content, formatter="text")
    with law.util.patch_object(sys, "path", [tmp_dir.path] + sys.path, lock=True):
        import cfg

    process = cms.Process(name)
    process.extend(cfg)

    return process

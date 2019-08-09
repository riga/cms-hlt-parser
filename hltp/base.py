# coding: utf-8

"""
HLT parser base tasks.
"""


__all__ = ["Task", "CMSSWSandbox", "BrilSandbox"]


import os
import re

import luigi
import law


law.contrib.load("telegram")


class Task(law.Task):
    """
    Custom base task that provides some common parameters and the :py:meth:`local_target`
    convenience method for automatically building target paths and wrapping them into local targets.
    """

    notify = law.NotifyTelegramParameter(significant=False)

    exclude_params_req = {"notify"}
    exclude_params_branch = {"notify"}
    exclude_params_workflow = {"notify"}
    message_cache_size = 20

    default_store = "$HLTP_STORE"

    def store_parts(self):
        parts = (self.task_family,)
        return parts

    def local_path(self, *path, **kwargs):
        # determine the path where to store targets
        store = kwargs.get("store") or self.default_store

        # get fragements / parts from store_parts and the passed path
        # which are translated into directories (["a", "b", "c"] -> "a/b/c")
        parts = [str(p) for p in self.store_parts() + path]

        # build the path and return it
        return os.path.expandvars(os.path.expanduser(os.path.join(store, *parts)))

    def local_target(self, *args, **kwargs):
        cls = law.LocalDirectoryTarget if kwargs.pop("dir", False) else law.LocalFileTarget
        return cls(self.local_path(*args, store=kwargs.pop("store", None)), **kwargs)


class CMSSWSandbox(law.SandboxTask):

    sandbox = "bash::$HLTP_BASE/hltp/files/env_cmssw.sh"


class BrilSandbox(law.SandboxTask):

    sandbox = "bash::$HLTP_BASE/hltp/files/env_bril.sh"

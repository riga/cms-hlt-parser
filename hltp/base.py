# coding: utf-8

"""
HLT parser base tasks.
"""


__all__ = ["Task", "TaskWithSummary", "CMSSWSandbox", "BrilSandbox"]


import os
import sys
import re
from subprocess import PIPE
from multiprocessing import Lock
from abc import abstractmethod
from contextlib import contextmanager

import luigi
import law

law.contrib.load("cms", "telegram", "wlcg")

from hltp.util import is_pattern


_summary_lock = Lock()


class Task(law.Task):
    """
    Custom base task that provides some common parameters and the :py:meth:`local_target`
    convenience method for automatically building target paths and wrapping them into local targets.
    """

    notify = law.telegram.NotifyTelegramParameter(significant=False)

    exclude_params_req = {"notify"}
    exclude_params_branch = {"notify"}
    exclude_params_workflow = {"notify"}
    message_cache_size = 20

    default_store = "$HLTP_STORE"

    check_for_patterns = []

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        for attr in self.check_for_patterns:
            for value in law.util.make_list(getattr(self, attr)):
                if is_pattern(value):
                    raise ValueError("the attribute {}.{} appears to contain a pattern: {}".format(
                        self.__class__.__name__, attr, value))

    def store_parts(self):
        return (self.task_family,)

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

    def call_step(self, cmd, msg, publish_cmd=False, prog=None):
        cmd = re.sub(r"\s+", " ", cmd).strip()
        with self.publish_step(msg, runtime=True):
            if publish_cmd:
                self.publish_message("cmd: {}".format(law.util.colored(cmd, style="bright")))

            code, out, _ = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=PIPE)

            if code != 0:
                raise Exception("{} failed".format(prog or cmd.split(" ", 1)[0]))

        return out


class TaskWithSummary(Task):
    """
    Task adding a *print_summary* parameter that, when set, only calls the tasks :py:meth:`summary`
    method instead of running anyhing. In law terms, this is an *interactive parameter*.
    """

    print_summary = luigi.BoolParameter(default=False, significant=False, description="print the "
        "task summary, do not run any task, requires the task to be complete, default: False")

    interactive_params = law.Task.interactive_params + ["print_summary"]
    exclude_params_req = {"print_summary"}

    def _print_summary(self, print_summary):
        if not self.complete():
            return True

        self.summary()

    @contextmanager
    def summary_lock(self):
        try:
            _summary_lock.acquire()
            yield
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            _summary_lock.release()

    def summary(self):
        print("print summary of task {}\n".format(self.repr()))

        if not self.complete():
            law.util.abort(msg="task not yet complete")


class CMSSWSandbox(law.SandboxTask):
    """
    Sandbox that embedds the run calls of inheriting tasks inside a CMSSW environment.
    """

    sandbox = "bash::$HLTP_BASE/hltp/files/env_cmssw.sh"


class BrilSandbox(law.SandboxTask):
    """
    Sandbox that embedds the run calls of inheriting tasks inside a bril environment.
    """

    sandbox = "bash::$HLTP_BASE/hltp/files/env_bril.sh"

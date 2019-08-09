# coding: utf-8

"""
HLT parser tasks.
"""


__all__ = []


import os
import re
from subprocess import PIPE

import luigi
import law

from hltp.base import Task, CMSSWSandbox, BrilSandbox


class TestTask(Task, BrilSandbox):

    def output(self):
        return self.local_target("output.txt")

    @law.decorator.notify
    def run(self):
        output = self.output()
        output.parent.touch()
        with output.open("w") as f:
            f.write(os.environ["PATH"] + "\n")
            f.write(os.environ["LAW_SANDBOX"] + "\n")


class GetLumiData(Task, BrilSandbox):

    lumi_file = luigi.Parameter(default=law.NO_STR, description="the lumi json file to use, "
        "default: config hltp.lumi_file")
    normtag_file = luigi.Parameter(default=law.NO_STR, description="the normtag file to use, "
        "default: config hltp.normtag_file")
    hlt_path = luigi.Parameter(description="the hlt path or pattern to query")
    force_summary = luigi.BoolParameter(default=False, description="force running to print the "
        "summary, might use the output when existing, default: False")

    def __init__(self, *args, **kwargs):
        super(GetLumiData, self).__init__(*args, **kwargs)

        if self.lumi_file == law.NO_STR:
            self.lumi_file = law.config.get("hltp", "lumi_file")
        if self.normtag_file == law.NO_STR:
            self.normtag_file = law.config.get("hltp", "normtag_file")

    def complete(self):
        return False if self.force_summary else super(GetLumiData, self).complete()

    def output(self):
        # replace pattern characters by "X"
        basename = self.hlt_path.replace("*", "X").replace("?", "X")
        return self.local_target("{}.json".format(basename))

    @law.decorator.notify
    def run(self):
        output = self.output()

        if not output.exists():
            # build the brilcal command to run
            cmd = """brilcalc lumi \
                -u /pb \
                -i {} \
                --normtag {} \
                --hltpath "{}"
            """.format(self.lumi_file, self.normtag_file, self.hlt_path)
            cmd = re.sub(r"\s+", " ", cmd).strip()

            # run it
            with self.publish_step("running brilcalc ..."):
                self.publish_message(cmd)

                code, out, _ = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash",
                    stdout=PIPE)

                if code != 0:
                    raise Exception("brilcalc failed")

            # parse the output
            lines = out[out.find("run:fill"):out.find("#Summary") - 1].strip().split("\n")[2:-1]
            data = {}
            for line in lines:
                # values are expected to be run:fill, time, ncms, hltpath, devlivered, recorded
                values = [part.strip() for part in line.strip()[1:-1].split("|")]
                run = int(values[0].split(":")[0])
                data[run] = dict(
                    run=run,
                    fill=int(values[0].split(":")[1]),
                    hlt_path=values[3],
                    lumi=float(values[5]),
                )

            output.parent.touch()
            output.dump(data, indent=4, formatter="json")
        else:
            data = output.load(formatter="json")

        # print some infos
        hlt_paths = sorted(list(set(d["hlt_path"] for d in data.values())))
        lumi = sum(d["lumi"] for d in data.values())
        print("")
        print("summary\n-------")
        print("runs : {}".format(len(data)))
        print("lumi : {:.3f} /pb".format(lumi))
        print("paths: {}".format(len(hlt_paths)))
        print("\n".join(("  - " + p) for p in hlt_paths))
        print("")


class GetLumiDataWrapper(Task, law.WrapperTask):

    lumi_file = GetLumiData.lumi_file
    normtag_file = GetLumiData.normtag_file
    hlt_paths = law.CSVParameter(description="hlt paths or patterns to query")

    def requires(self):
        return [GetLumiData.req(self, hlt_path=p) for p in self.hlt_paths]

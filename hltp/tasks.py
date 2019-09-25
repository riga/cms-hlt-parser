# coding: utf-8

"""
HLT parser tasks.
"""


import os
import sys
import re
import collections
import itertools

import six
import luigi
import law
import tabulate

from hltp.base import Task, TaskWithSummary, CMSSWSandbox, BrilSandbox
from hltp.util import expand_pset, fwlite_loop, text_to_process


luigi.namespace("hltp", scope=__name__)


class GetDatasetLFNs(TaskWithSummary, CMSSWSandbox):

    dataset = luigi.Parameter(description="the dataset to query")

    def output(self):
        return self.local_target("{}.json".format(self.dataset.replace("/", "_")))

    @law.decorator.notify
    @law.wlcg.ensure_voms_proxy
    @law.decorator.localize
    def run(self):
        # run dasgoclient
        cmd = "dasgoclient --query 'file dataset={}' --limit 0".format(self.dataset)
        out = self.call_step(cmd, "get dataset files ...")

        # parse the output
        files = [f.strip() for f in out.strip().split("\n")]

        # save the output and print the summary
        self.output().dump(files, indent=4, formatter="json")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GetDatasetLFNs, self).summary()

            files = self.output().load(formatter="json")

            print("files: {}".format(len(files)))
            print("")


class GetDatasetLFNsWrapper(Task, law.WrapperTask):

    datasets = law.CSVParameter(default=law.config.keys("hltp_mc_datasets"),
        description="datasets to query, default: config hltp_mc_datasets")

    def requires(self):
        return [GetDatasetLFNs.req(self, dataset=d) for d in self.datasets]


class GetLumiData(TaskWithSummary, BrilSandbox):

    hlt_path = luigi.Parameter(description="the hlt path (can be a pattern) to query")
    lumi_file = luigi.Parameter(default=law.config.get("hltp_config", "lumi_file"),
        description="the lumi json file to use, default: config hltp_config.lumi_file")
    normtag_file = luigi.Parameter(default=law.config.get("hltp_config", "normtag_file"),
        description="the normtag file to use, default: config hltp_config.normtag_file")

    def output(self):
        # replace pattern characters by "X"
        basename = self.hlt_path.replace("*", "X").replace("?", "X")
        return self.local_target("{}.json".format(basename))

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        # build the brilcal command to run
        cmd = """brilcalc lumi \
            -u /pb \
            -i {} \
            --normtag {} \
            --hltpath "{}"
        """.format(self.lumi_file, self.normtag_file, self.hlt_path)

        # run it
        out = self.call_step(cmd, "running brilcalc lumi ...", publish_cmd=True)

        # parse the output
        lines = out[out.find("run:fill"):out.find("#Summary") - 1].strip().split("\n")[2:-1]
        data = {}
        for line in lines:
            # values are expected to be run:fill, time, ncms, hltpath, devlivered, recorded
            values = [part.strip() for part in line.strip()[1:-1].split("|")]
            run = int(values[0].split(":")[0])
            data[run] = dict(
                lumi=float(values[5]),
                hlt_path=values[3],
            )

        # save the output and print the summary
        self.output().dump(data, indent=4, formatter="json")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GetLumiData, self).summary()

            data = self.output().load(formatter="json")

            hlt_paths = sorted(list(set(d["hlt_path"] for d in data.values())))
            lumi = sum(d["lumi"] for d in data.values())
            print("runs : {}".format(len(data)))
            print("lumi : {:.3f} /pb".format(lumi))
            print("paths: {}".format(len(hlt_paths)))
            print("\n".join(("  - " + p) for p in hlt_paths))
            print("")


class GetLumiDataWrapper(Task, law.WrapperTask):

    lumi_file = GetLumiData.lumi_file
    normtag_file = GetLumiData.normtag_file
    hlt_paths = law.CSVParameter(default=law.config.keys("hltp_paths"),
        description="hlt paths (can be patterns) to query, default: config hltp_paths")

    def requires(self):
        return [GetLumiData.req(self, hlt_path=p) for p in self.hlt_paths]


class GetMenusFromDataset(TaskWithSummary, CMSSWSandbox):

    dataset = luigi.Parameter(description="the dataset to query")
    file_index = luigi.IntParameter(default=0, description="the index of the dataset file to use, "
        "default: 0")

    def requires(self):
        return GetDatasetLFNs.req(self)

    def output(self):
        path = "{}_{}.json".format(self.dataset.replace("/", "_"), self.file_index)
        return self.local_target(path)

    @law.decorator.notify
    @law.wlcg.ensure_voms_proxy
    @law.decorator.localize
    def run(self):
        # get the dataset lfn
        lfn = self.input().load(formatter="json")[self.file_index]

        # use the global xrd redirector to treat it as a physics file name
        pfn = law.cms.lfn_to_pfn(lfn)

        # run hltInfo
        cmd = "hltInfo {}".format(pfn)
        out = self.call_step(cmd, "run htlInfo ...")

        # parse the output to extract the hlt menus
        menus = []
        lines = [line.strip() for line in out.strip().split("\n")]
        for line in lines:
            match = re.match(r"^HLT\smenu\:\s+'(/.+)'$", line)
            if match:
                menus.append(match.group(1))

        if not menus:
            raise Exception("could not extract any menu from hltInfo output")

        # save the output and print the summary
        self.output().dump(menus, indent=4, formatter="json")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GetMenusFromDataset, self).summary()

            menus = self.output().load(formatter="json")

            print("menus: {}".format(len(menus)))
            for m in menus:
                print("  - {}".format(m))
            print("")


class GetMenusFromDatasetWrapper(Task, law.WrapperTask):

    datasets = law.CSVParameter(default=law.config.keys("hltp_mc_datasets"),
        description="datasets to query, default: config hltp_mc_datasets")
    file_indices = law.CSVParameter(default=[0], description="indices of dataset files to query, "
        "default: [0]")

    def requires(self):
        return [
            GetMenusFromDataset.req(self, dataset=d, file_index=f)
            for d, f in itertools.product(self.datasets, self.file_indices)
        ]


class GetDataMenus(TaskWithSummary, BrilSandbox):

    show_runs = luigi.BoolParameter(default=False, significant=False, description="when set, print "
        "all run numbers instead of their count in the summary, default: False")

    def output(self):
        return self.local_target("menus.json")

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        # build the brilcalc command
        cmd = "brilcalc trg --output-style csv"
        out = self.call_step(cmd, "run brilcalc trg ...")

        # parse the output
        data = collections.OrderedDict()
        for line in out.strip().split("\n")[1:]:
            line = line.strip()
            if not line:
                continue
            menu_id, menu_name, runs = line.split(",", 2)
            menu_id = int(menu_id)
            runs = sorted([int(r.strip()) for r in runs[1:-1].split(",")])
            data[menu_name] = {"menu_id": menu_id, "runs": runs}

        # save the output and print the summary
        self.output().dump(data, indent=4, formatter="json")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GetDataMenus, self).summary()

            data = self.output().load(formatter="json")
            menus = sorted([str(key) for key in data.keys()], key=str.lower)

            print("menus: {}".format(len(data)))
            print("runs : {}".format(sum(len(d["runs"]) for d in data.values())))
            for menu in menus:
                if self.show_runs:
                    runs_str = ": " + ",".join(str(r) for r in data[menu]["runs"])
                else:
                    runs_str = "({} runs)".format(str(len(data[menu]["runs"])))
                print("  - {} {}".format(menu, runs_str))
            print("")


class GetPathsFromDataset(TaskWithSummary, CMSSWSandbox):

    dataset = GetMenusFromDataset.dataset
    file_index = GetMenusFromDataset.file_index

    def requires(self):
        return GetDatasetLFNs.req(self)

    def output(self):
        path = "{}_{}.json".format(self.dataset.replace("/", "_"), self.file_index)
        return self.local_target(path)

    @law.decorator.notify
    @law.wlcg.ensure_voms_proxy
    @law.decorator.localize
    def run(self):
        # get the dataset lfn
        lfn = self.input().load(formatter="json")[self.file_index]

        # convert it to a pfn
        pfn = law.lfn_to_pfn(lfn)

        # stream and open one event with fwlite
        handle_data = {
            "trigger_bits": {
                "type": "edm::TriggerResults",
                "label": ("TriggerResults", "", "HLT"),
            },
        }
        for event, data in fwlite_loop(pfn, handle_data, end=1):
            triggers = event.object().triggerNames(data["trigger_bits"])
            triggers = sorted([triggers.triggerName(i) for i in range(triggers.size())])

        # save the output and print the summary
        self.output().dump(triggers, indent=4, formatter="json")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GetPathsFromDataset, self).summary()

            triggers = self.output().load(formatter="json")

            print("triggers: {}".format(len(triggers)))
            for t in triggers:
                print("  - {}".format(t))
            print("")


class GetPathsFromDatasetWrapper(Task, law.WrapperTask):

    datasets = GetMenusFromDatasetWrapper.datasets
    file_indices = GetMenusFromDatasetWrapper.file_indices

    def requires(self):
        return [
            GetPathsFromDataset.req(self, dataset=d, file_index=f)
            for d, f in itertools.product(self.datasets, self.file_indices)
        ]


class GetPathsFromMenu(TaskWithSummary, CMSSWSandbox):

    hlt_menu = luigi.Parameter(description="the hlt menu to query")

    def output(self):
        path = "{}.json".format(self.hlt_menu.replace("/", "_"))
        return self.local_target(path)

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        # get the config from the hlt db
        cmd = "hltConfigFromDB --configName {} --cff".format(self.hlt_menu)
        if re.match(r"^/cdaq/", self.hlt_menu):
            cmd = "{} --adg".format(cmd)
        out = self.call_step(cmd, "getting the config from the hlt db ...", publish_cmd=True)

        # parse the last line that defines the HLTSchedule
        schedule = out.strip().split("\n")[-1].strip().replace(" ", "")
        if not schedule.startswith("HLTSchedule=cms.Schedule"):
            raise Exception("could not find the HLTSchedule, the config is probably wrong")
        triggers = schedule[schedule.find("*") + 2:schedule.find("))")].split(",")

        # remove modules that do not start with HLT_ and sort
        triggers = sorted([t for t in triggers if t.startswith("HLT_")])

        # save the output and print the summary
        self.output().dump(triggers, indent=4, formatter="json")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GetPathsFromMenu, self).summary()

            triggers = self.output().load(formatter="json")

            print("triggers: {}".format(len(triggers)))
            for t in triggers:
                print("  - {}".format(t))
            print("")


class GetFilterNamesFromMenu(TaskWithSummary, CMSSWSandbox):

    hlt_menu = luigi.Parameter(description="the hlt menu (no pattern!) to query")
    hlt_path = luigi.Parameter(description="the hlt path (no pattern!) to query")

    def output(self):
        prefix = "{}__{}".format(self.hlt_menu.replace("/", "_"), self.hlt_path)
        return {
            "filters": self.local_target("{}.json".format(prefix)),
            "config": self.local_target("{}.py".format(prefix)),
        }

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        import FWCore.ParameterSet.Config as cms
        from FWCore.ParameterSet.Modules import EDFilter

        # get the config from the hlt db
        cmd = "hltConfigFromDB --configName {} --paths {} --cff".format(
            self.hlt_menu, self.hlt_path)
        if re.match(r"^/cdaq/", self.hlt_menu):
            cmd = "{} --adg".format(cmd)
        out = self.call_step(cmd, "getting the config from the hlt db ...", publish_cmd=True)

        # remove the HLTSchedule line as it often does not contain valid python syntax
        stop = out.find("HLTSchedule")
        if stop < 0:
            raise Exception("could not find the HLTSchedule, the config is probably wrong")
        content = out[:stop]

        # convert the cfg content into a cms.Process
        process = text_to_process(content)

        # get the path describing the hlt path, loop through modules and save filters
        p = getattr(process, self.hlt_path)
        module_names = str(p).split("+")
        filters = []
        for name in module_names:
            mod = getattr(process, name, None)
            if isinstance(mod, EDFilter):
                filters.append({"name": name, "parameters": expand_pset(mod.parameters_())})

        # save the output and print the summary
        outputs = self.output()
        outputs["filters"].dump(filters, indent=4, formatter="json")
        outputs["config"].dump(content, formatter="text")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GetFilterNamesFromMenu, self).summary()

            filters = self.output()["filters"].load(formatter="json")

            print("filters: {}".format(len(filters)))
            for f in filters:
                print("  - {}".format(f["name"]))
            print("")


class GetFilterNamesFromMenuWrapper(Task, law.WrapperTask):

    hlt_menus = law.CSVParameter(description="hlt menus (no patterns!) to query")
    hlt_paths = law.CSVParameter(description="hlt paths (no patterns!) to query")

    def requires(self):
        return [
            GetFilterNamesFromMenu.req(self, hlt_menu=m, hlt_path=p)
            for m, p in itertools.product(self.hlt_menus, self.hlt_paths)
        ]


class GetFilterNamesFromRun(TaskWithSummary):

    hlt_path = GetFilterNamesFromMenu.hlt_path
    run_number = luigi.IntParameter(description="the run to query")

    def __init__(self, *args, **kwargs):
        super(GetFilterNamesFromRun, self).__init__(*args, **kwargs)

        self.hlt_menu = None

    def requires(self):
        return GetDataMenus.req(self)

    def output(self):
        prefix = "run{}__{}".format(self.run_number, self.hlt_path)
        return {
            "filters": self.local_target("{}.json".format(prefix)),
            "config": self.local_target("{}.py".format(prefix)),
        }

    @law.decorator.notify
    def run(self):
        # as this method is a generator, i.e., it yields a dynamic dependency, it should be
        # idempotent, so only get the hlt menu when this was not done so far
        if not self.hlt_menu:
            # load the menu data and determine the name of the menu that was used for the run
            menu_data = self.input().load(formatter="json")
            for hlt_menu, data in menu_data.items():
                if self.run_number in data["runs"]:
                    self.hlt_menu = hlt_menu
                    self.publish_message("found trigger menu {} for run {}".format(
                        hlt_menu, self.run_number))
                    break
            else:
                raise Exception("no hlt menu found that contains run {}".format(self.run))

        # declare GetFilterNamesFromMenu for that menu as a dynamic dependency
        targets = yield GetFilterNamesFromMenu.req(self, hlt_menu=self.hlt_menu)

        # the result of GetFilterNamesFromMenu is identical to the result of _this_ task, so copy it
        outputs = self.output()
        outputs["filters"].copy_from_local(targets["filters"])
        outputs["config"].copy_from_local(targets["config"])
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GetFilterNamesFromRun, self).summary()

            filters = self.output()["filters"].load(formatter="json")

            print("filters: {}".format(len(filters)))
            for f in filters:
                print("  - {}".format(f["name"]))
            print("")


class GetFilterNamesFromRunWrapper(Task, law.WrapperTask):

    run_numbers = law.CSVParameter(cls=luigi.IntParameter, description="run numbers to query")
    hlt_paths = law.CSVParameter(description="hlt paths (no patterns!) to query")

    def requires(self):
        return [
            GetFilterNamesFromRun.req(self, run_number=r, hlt_path=p)
            for r, p in itertools.product(self.run_numbers, self.hlt_paths)
        ]


class GatherMCFilters(TaskWithSummary):

    datasets = GetMenusFromDatasetWrapper.datasets
    hlt_paths = GetLumiDataWrapper.hlt_paths
    verbose_datasets = luigi.BoolParameter(default=False, significant=False, description="when "
        "set, print full dataset names in the first summary table, default: False")
    table_format = luigi.Parameter(default="grid", significant=False, description="the tabulate "
        "table format for the summary, default: grid")

    def output(self):
        return self.local_target("filters.json")

    @law.decorator.notify
    def run(self):
        # strategy:
        # 1. Get all menus for all datasets.
        # 2. Assume that each dataset contains only one menu.
        # 3. Get all paths for all menus.
        # 4. Use patterns defined in hlt_paths to select paths obtained in (4).
        # 5. Get filter names for each menu and each selected path.
        # 6. Save the data.

        # coloring and colored formatter helpers
        col = lambda s: law.util.colored(s, color="light_blue", style="bright")
        fmt = lambda s, *args: s.format(*(col(arg) for arg in args))

        # 1
        menu_inputs = yield [
            GetMenusFromDataset.req(self, dataset=dataset)
            for dataset in self.datasets
        ]

        # 2
        menu_datasets = {}
        for dataset, inp in zip(self.datasets, menu_inputs):
            _menus = inp.load(formatter="json")
            if len(_menus) != 1:
                raise Exception("MC datasets are expected to contain one HLT menu, got {}".format(
                    ",".join(_menus)))
            menu_datasets.setdefault(str(_menus[0]), []).append(dataset)
        menus = sorted(list(menu_datasets.keys()), key=str.lower)

        # 3
        paths_inputs = yield {
            menu: GetPathsFromMenu.req(self, hlt_menu=menu)
            for menu in menus
        }
        all_paths = {
            menu: inp.load(formatter="json")
            for menu, inp in six.iteritems(paths_inputs)
        }

        # 4
        menu_paths = {
            menu: [str(p) for p in paths if law.util.multi_match(p, self.hlt_paths, mode=any)]
            for menu, paths in six.iteritems(all_paths)
        }
        self.publish_message(fmt("selected {} from {} available path(s) in {} menu(s):",
            len(law.util.flatten(menu_paths.values())),
            len(law.util.flatten(all_paths.values())), len(menus)))
        for menu, paths in six.iteritems(menu_paths):
            self.publish_message(fmt("{}:\n    {}", menu, "\n    ".join(paths)))

        # 5
        menu_path_pairs = sum((
            [(menu, path) for path in paths]
            for menu, paths in six.iteritems(menu_paths)
        ), [])
        filter_inputs = yield {
            (menu, path): GetFilterNamesFromMenu.req(self, hlt_menu=menu, hlt_path=path)
            for menu, path in menu_path_pairs
        }
        filter_names = {
            key: [str(f["name"]) for f in inps["filters"].load(formatter="json")]
            for key, inps in six.iteritems(filter_inputs)
        }

        # 6
        data = []
        for menu, datasets in six.iteritems(menu_datasets):
            data.append(dict(
                menu=menu,
                datasets=datasets,
                paths=[
                    dict(
                        name=path,
                        filters=filter_names[(menu, path)],
                    )
                    for path in menu_paths[menu]
                ],
            ))

        # save the output and print the summary
        output = self.output()
        output.parent.touch()
        output.dump(data, indent=4, formatter="json")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GatherMCFilters, self).summary()

            # read data
            data = self.output().load(formatter="json")

            # print the menu - dataset table
            headers = ["HLT path(s)", "HLT menu", "Dataset(s)"]
            rows = []
            for entry in data:
                datasets = entry["datasets"]
                if not self.verbose_datasets:
                    datasets = [d.split("/", 2)[1] for d in datasets]
                rows.append([
                    "\n".join(p["name"] for p in entry["paths"]),
                    entry["menu"],
                    "\n".join(datasets),
                ])

            rows = sorted(rows, key=lambda row: row[0].lower())

            print(tabulate.tabulate(rows, headers=headers, tablefmt=self.table_format))
            print("")

            # get a flat (menu, path) -> filters map
            filter_map = {}
            for entry in data:
                for path_entry in entry["paths"]:
                    filter_map[(entry["menu"], path_entry["name"])] = path_entry["filters"]

            # define the table
            headers = ["HLT path(s)", "HLT menu(s)", "Filter names"]
            rows = []

            # when multiple paths have exactly the same filter names associated, we want to reflect
            # that in the printed table, so use a while loop and a reduction pattern
            keys = sorted(list(filter_map.keys()), key=lambda key: key[1].lower())
            while keys:
                key = keys.pop(0)
                menu, path = key

                # prepare paths, menus and filter names
                paths = [path]
                menus = [menu]
                filters = tuple(filter_map[key])

                # try to look ahead to check if there is another entry with the same names
                for _key in list(keys):
                    _menu, _path = _key
                    if tuple(filter_map[_key]) == filters:
                        keys.remove(_key)
                        paths.append(_path)
                        menus.append(_menu)

                rows.append([
                    "\n".join(paths),
                    "\n".join(menus),
                    "\n".join(filters),
                ])

            print(tabulate.tabulate(rows, headers=headers, tablefmt=self.table_format))


class GatherDataFilters(TaskWithSummary):

    lumi_file = GetLumiData.lumi_file
    hlt_menus = law.CSVParameter(default=law.config.keys("hltp_data_menus"),
        description="hlt menus (can be patterns) to query")
    hlt_paths = GetLumiDataWrapper.hlt_paths
    show_menus = luigi.BoolParameter(default=False, significant=False, description="if set, show "
        "an additional 'HLT menu(s)' column in the summary table, default: False")
    verbose_runs = luigi.BoolParameter(default=False, significant=False, description="if set, "
        "print the full list of run umbers in the summary table, default: False")
    table_format = GatherMCFilters.table_format

    def output(self):
        return self.local_target("filters.json")

    @law.decorator.notify
    def run(self):
        # strategy:
        # 1. Get the list of valid run numbers from the lumi file.
        # 2. Get all menus and associated runs, and filter the latter by (1).
        # 3. Filter menus using the provided menu patterns.
        # 4. For all menus, get the list of paths and filter them using the provided path patterns.
        # 5. Get filter names for each menu and path combination.
        # 6. Save the data.

        # coloring and colored formatter helpers
        col = lambda s: law.util.colored(s, color="light_blue", style="bright")
        fmt = lambda s, *args: s.format(*(col(arg) for arg in args))

        # 1
        lumi_data = law.LocalFileTarget(self.lumi_file).load(formatter="json")
        valid_runs = [
            int(run) for run, section in six.iteritems(lumi_data)
            if law.util.flatten(section)
        ]
        self.publish_message(fmt("found {} valid runs in lumi file", len(valid_runs)))

        # 2
        all_menu_runs = (yield GetDataMenus.req(self)).load(formatter="json")
        menu_runs = {
            menu: [
                run for run in data["runs"]
                if run in valid_runs
            ]
            for menu, data in six.iteritems(all_menu_runs)
        }

        # 3
        menu_runs = {
            menu: runs for menu, runs in six.iteritems(menu_runs)
            if runs and law.util.multi_match(menu, self.hlt_menus, mode=any)
        }
        self.publish_message(fmt("found a total of {} valid runs in {} menus:\n{} ",
            sum(len(runs) for runs in six.itervalues(menu_runs)), len(menu_runs),
            "\n".join(menu_runs.keys())))

        # 4
        paths_inputs = yield {
            menu: GetPathsFromMenu.req(self, hlt_menu=menu)
            for menu in menu_runs
        }
        menu_paths = {
            menu: [
                p for p in inp.load(formatter="json")
                if law.util.multi_match(p, self.hlt_paths)
            ]
            for menu, inp in six.iteritems(paths_inputs)
        }

        # 5
        menu_path_pairs = sum((
            [(menu, path) for path in paths]
            for menu, paths in six.iteritems(menu_paths)
        ), [])
        filter_inputs = yield {
            (menu, path): GetFilterNamesFromMenu.req(self, hlt_menu=menu, hlt_path=path)
            for menu, path in menu_path_pairs
        }
        filter_names = {
            (menu, path): [d["name"] for d in inps["filters"].load(formatter="json")]
            for (menu, path), inps in six.iteritems(filter_inputs)
        }

        # 6
        data = []
        for menu, runs in six.iteritems(menu_runs):
            data.append(dict(
                menu=menu,
                runs=runs,
                paths=[
                    dict(
                        name=path,
                        filters=filter_names[(menu, path)],
                    )
                    for path in menu_paths[menu]
                ],
            ))

        # save the output and print the summary
        output = self.output()
        output.parent.touch()
        output.dump(data, indent=4, formatter="json")
        self.summary()

    def summary(self):
        with self.summary_lock():
            super(GatherDataFilters, self).summary()

            # read data
            data = self.output().load(formatter="json")

            # get a menu -> runs map
            menu_runs = {entry["menu"]: entry["runs"] for entry in data}

            # get a flat (menu, path) -> filters map
            filter_map = {}
            for entry in data:
                for path_entry in entry["paths"]:
                    filter_map[(entry["menu"], path_entry["name"])] = path_entry["filters"]

            # helper to compress run numbers into a readable string
            def compress_runs(runs):
                if len(runs) == 1:
                    return str(runs[0])
                else:
                    runs = sorted(runs)
                    return "{}-{}".format(runs[0], runs[-1])

            # define the table
            headers = ["HLT path(s)", "Runs", "HLT menu(s)", "Filter names"]
            rows = []

            # when multiple paths have exactly the same filter names associated, we want to reflect
            # that in the printed table, so use a while loop and a reduction pattern
            keys = [(str(menu), str(path)) for menu, path in filter_map]
            while keys:
                key = keys.pop(0)
                menu, path = key

                # prepare paths, menus, runs and filter names
                paths = [path]
                menus = [menu]
                runs = [sorted(menu_runs[menu])]
                filters = tuple(filter_map[key])

                # try to look ahead to check if there is another entry with the same names
                for _key in list(keys):
                    _menu, _path = _key
                    if tuple(filter_map[_key]) == filters:
                        keys.remove(_key)
                        paths.append(_path)
                        menus.append(_menu)
                        runs.append(sorted(menu_runs[_menu]))

                # create string entries
                if self.verbose_runs:
                    paths_str = "\n".join(sorted(list(set(paths))))
                    unique_runs = list(set(law.util.flatten(runs)))
                    runs_str = ",\n".join(
                        ",".join(str(r) for r in chunk)
                        for chunk in law.util.iter_chunks(sorted(unique_runs), 5)
                    )
                else:
                    # some additional sorting
                    compressed_runs = [compress_runs(_runs) for _runs in runs]
                    indices = list(range(len(paths)))
                    sorted_indices = sorted(indices, key=lambda i: (paths[i], compressed_runs[i]))
                    paths_str = "\n".join(paths[i] for i in sorted_indices)
                    runs_str = "\n".join(compressed_runs[i] for i in sorted_indices)
                menus_str = "\n".join(sorted(list(set(menus)), key=str.lower))
                filters_str = "\n".join(filters)

                # append a new row
                rows.append([paths_str, runs_str, menus_str, filters_str])

            # sort rows by the first path
            rows = sorted(rows, key=lambda row: row[0].split("\n", 1)[0])

            # remove the menu column if requested
            if not self.show_menus:
                headers.pop(2)
                for row in rows:
                    row.pop(2)

            print(tabulate.tabulate(rows, headers=headers, tablefmt=self.table_format))

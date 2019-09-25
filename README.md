# cms-hlt-parser

Read, parse and provide CMS High-Level Trigger and Luminosity information such as trigger menus, trigger paths, filter names, etc.

The tasks defined in this project are based on the [luigi analysis workflow(law)](https://law.readthedocs.io/en/latest) package. For more information, see

- [law](https://law.readthedocs.io/en/latest)
- [luigi](https://luigi.readthedocs.io/en/stable)

Available tasks:

- [`GetDatasetLFNs`](#getdatasetlfns)
- [`GetLumiData`](#getlumidata)
- [`GetMenusFromDataset`](#getmenusfromdataset)
- [`GetDataMenus`](#getdatamenus)
- [`GetPathsFromDataset`](#getpathsfromdataset)
- [`GetPathsFromMenu`](#getpathsfrommenu)
- [`GetFilterNamesFromMenu`](#getfilternamesfrommenu)
- [`GetFilterNamesFromRun`](#getfilternamesfromrun)
- [`GatherMCFilters`](#gathermcfilters)
- [`GatherDataFilters`](#gatherdatafilters)


### Setup

```bash
git clone https://github.com/riga/cms-hlt-parser
cd cms-hlt-parser

# a proxy is required for communicating with DAS and the HLT database
voms-proxy-init -voms cms

# always run the setup (which installs some software _once_)
source setup
```


### Tasks

To execute a task, run `law run hltp.<task_name> [parameters]`. Please find a list of all available tasks, parameters and examples below.

Also, some task parameter have defaults that lookup values in the [law.cfg](./law.cfg) config file.

The output file(s) of a task can be obtained by adding `--print-status 0` or `--print-output 0` to the respective `law run` command. They are mostly JSON files containing the described output information. Also, almost all tasks print a summary after they successfully run.

In case a task is already completed (i.e., its output exists), it is not run again and no summary is printed. To print only the summary, add `--print-summary` to the `law run` command.


##### `GetDatasetLFNs`

> `> law run hltp.GetDatasetLFNs [parameters]`

Queries the DAS service to get the logical file names (LFNs) for a dataset (e.g. `/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/.../MINIAODSIM`). The output is the list of LFNs.

Parameters:

- `--dataset`: A dataset as shown in DAS. Patterns not supported.


##### `GetLumiData`

> `> law run hltp.GetLumiData [parameters]`

Uses `brilcalc lumi` to get luminosity data for a trigger path (name or pattern), given luminosity and normtag files. The output is a dictionary `run -> {lumi, hlt_path}`.

Parameters:

- `--hlt-path`: A trigger path. Patterns allowed.
- `--lumi-file`: A luminosity file. Defaults to the `hltp_config.lumi_file` config.
- `--normtag-file`: A normtag file. Defaults to the `hltp_config.normtag_file` config.


##### `GetMenusFromDataset`

> `> law run hltp.GetMenusFromDataset [parameters]`

Returns the menus found in the n-th file of a dataset using the `hltInfo` command. The output is a list of trigger menus.

Parameters:

- `--dataset`: A dataset as shown in DAS. Patterns not supported.
- `--file-index`: The number of the file to query. Defaults to 0.


##### `GetDataMenus`

> `> law run hltp.GetDataMenus [parameters]`

Uses `brilcalc trg` to obtain all trigger menus used for data-taking and maps them to the runs the menus they were used in. The ouptut is a dictionary `menu_name -> {menu_id, runs}`.

Parameters:

*No parameters*.


##### `GetPathsFromDataset`

> `> law run hltp.GetPathsFromDataset [parameters]`

Returns the triggers paths found in the n-th file of a dataset. Internally, this is done by opening the file with `fwlite` and reading the trigger names using the trigger bits. The output is a list of trigger paths.

Parameters:

- `--dataset`: A dataset as shown in DAS. Patterns not supported.
- `--file-index`: The number of the file to query. Defaults to 0.


##### `GetPathsFromMenu`

> `> law run hltp.GetPathsFromMenu [parameters]`

Returns the triggers paths for a trigger menu `hltConfFromDB`. The output is a list of trigger paths.

Parameters:

- `--hlt-menu`: The trigger menu to query. Patterns not supported.


##### `GetFilterNamesFromMenu`

> `> law run hltp.GetFilterNamesFromMenu [parameters]`

Gets the names of all `EDFilter` modules of a trigger path as defined in a trigger menu. Internally, `hltConfigFromDB` is used to obtain the configuration file of the path, the configuration is loaded, and the filter modules are identified programmatically. The output is a list of dictionaries `{name, parameters}`.

Parameters:

- `--hlt-path`: The trigger path to query. Patterns not supported.
- `--hlt-menu`: The trigger menu to query. Patterns not supported.


##### `GetFilterNamesFromRun`

> `> law run hltp.GetFilterNamesFromRun [parameters]`

Gets the names of all `EDFilter` modules of a trigger path used in a specific run. Internally, `GetDataMenus` is used to make the connection between trigger menu and run number, and `GetFilterNamesFromMenu` is invoked internally as a [dynamic dependency](https://luigi.readthedocs.io/en/stable/tasks.html?highlight=dynamic#dynamic-dependencies). In fact, the output is identical to `GetFilterNamesFromMenu`, i.e., it is a list of dictionaries `{name, parameters}`.

Parameters:

- `--hlt-path`: The trigger path to query. Patterns not supported.
- `--run-number`: The run number to query.


##### `GatherMCFilters`

> `> law run hltp.GatherMCFilters [parameters]`

Gathers information about the `EDFilter` names of several trigger paths used for MC production. The menus are obtained by querying the respective first files of some datasets. The ouptut is a list of dictionaries `{menu, datasets, paths}`, where `paths` itself is a list of dictionaries `{name, filters}`. The summary consists of two tables. The first table shows which `"HLT path(s)"` belong to which `"HLT menu"` as found in which `"Dataset(s)"`. The second table contains the actual filter names. The columns are `"HLT path(s)"`, `"HLT menu(s)"` and `"Filter names"`.

Parameters:

- `--datasets`: The MC datasets to query. Patterns not supported. Defaults to the entries in the `hltp_mc_datasets` config.
- `--hlt-paths`: The trigger paths to query. Patterns allowed. Defaults to the entries in the `hltp_paths` config.
- `--verbose-datasets`: When used, the full dataset names are printed in the first summary table. Defaults to false.
- `--table-format`: The [tabulate](https://pypi.org/project/tabulate/) table format. Defaults to `"grid"`.

Example output:

![MC filters](https://www.dropbox.com/s/jv4y5sdhfvy6ars/hltp_mc_filters.png.png?raw=1)


##### `GatherDataFilters`

> `> law run hltp.GatherDataFilters [parameters]`

Gathers information about the `EDFilter` names of several trigger paths in several trigger menus employed for data-taking. Valid run numbers are taken from a luminosity file. The ouptut is a list of dictionaries `{menu, runs, paths}`, where `paths` itself is a list of dictionaries `{name, filters}`. The summary table has the columns `"HLT path(s)"`, `"Runs"`, and `"Filter names"`, and optionally `"HLT menu(s)"`.

Parameters:

- `--hlt-menus`: The trigger menus to query. Patterns allowed. Defaults to the entries in the `hltp_data_menus` config.
- `--hlt-paths`: The trigger paths to query. Patterns allowed. Defaults to the entries in the `hltp_paths` config.
- `--lumi-file`: A luminosity file. Defaults to the `hltp_config.lumi_file` config.
- `--show-menus`: When used, the optional `"HLT menu(s)"` column is shown in the second summary table. Defaults to false.
- `--verbose-runs`: When used, the run numbers in the second summary table are not contracted to ranges. Defaults to false.
- `--table-format`: The [tabulate](https://pypi.org/project/tabulate/) table format. Defaults to `"grid"`.

Example output:

![Data filters](https://www.dropbox.com/s/m71xbel9pkzux2k/hltp_data_filters.png?raw=1)

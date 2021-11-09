# Test instructions.
You need a properly working python3 interpreter installed. we have seen issues where the python3 installation was a problem.

You need credential files in your home directory which can be retrieved (if you have permission) like this if you have cd'd into your home directory.

```scp www-data@model-vxtest.gsd.esrl.noaa.gov:~/adb-cb* .```

You also have to export the PYTHONPATH to be the top level VxIngest directory (the directory where you cloned the repo).
for bash...
```cd the_clone_dir_for_VxIngest; export PYTHONPATH=\`pwd\'```

## environment

You can create a python virtual environment in the test_env directory of this repo.
To create and activate the virtual environment ...

- ```cd top_level_of_repo  (like ~/VxIngest) - where you cloned the repo.```
- ```python3 -m venv test_venv```
- ```source test_env/bin/activate```

If you have difficulty refer to <https://docs.python.org/3/tutorial/venv.html>

Once you hacve sourced the environment you can load all of the necessary packages with

- ```pip install -r requirements.txt```

## tests

There are two kinds of tests in each test directory.

- integration - tests are named like grib2_to_cb/test/test_int_metar_model_grib.py
- unit - tests are named like grib2_to_cb/test/test_unit_metar_model_grib.py

Notice the ***test_int_*** and the ***test_unit*** in the names.
Unit tests are relatively independent, do not require external test data, and run quickly. These tests are for testing methods or functions independantly. Integration tests require external data and configuration, are not independant, and may be long running. These tests are for testing a working system, or components of a working system that are interacting.

## vscode

We use several extensions in vscode. This is a sample list of useful extensions. This list can be used directly to install extensions on a different machine. This list is also checked into the test_env directory in the file ```vscode_extensions.sh```. You can install all of the extensions in that list by executing that file. You can also sinstall them individually by picking and choosing from the list.

``` sh
code --install-extension alefragnani.project-manager
code --install-extension alexkrechik.cucumberautocomplete
code --install-extension cweijan.vscode-mysql-client2
code --install-extension DavidAnson.vscode-markdownlint
code --install-extension dbaeumer.vscode-eslint
code --install-extension eamodio.gitlens
code --install-extension esbenp.prettier-vscode
code --install-extension foxundermoon.shell-format
code --install-extension GitHub.vscode-pull-request-github
code --install-extension hashicorp.terraform
code --install-extension hbenl.vscode-test-explorer
code --install-extension hbenl.vscode-test-explorer-liveshare
code --install-extension HookyQR.beautify
code --install-extension isudox.vscode-jetbrains-keybindings
code --install-extension ivanhofer.git-assistant
code --install-extension littlefoxteam.vscode-python-test-adapter
code --install-extension mhutchie.git-graph
code --install-extension ms-azuretools.vscode-docker
code --install-extension ms-python.python
code --install-extension ms-python.vscode-pylance
code --install-extension ms-toolsai.jupyter
code --install-extension ms-toolsai.jupyter-keymap
code --install-extension ms-toolsai.jupyter-renderers
code --install-extension ms-vscode-remote.remote-containers
code --install-extension ms-vscode-remote.remote-ssh
code --install-extension ms-vscode-remote.remote-ssh-edit
code --install-extension ms-vscode.test-adapter-converter
code --install-extension ms-vsliveshare.vsliveshare
code --install-extension msjsdiag.debugger-for-chrome
code --install-extension naumovs.color-highlight
code --install-extension njpwerner.autodocstring
code --install-extension ramonitor.meteorhelper
code --install-extension shardulm94.trailing-spaces
code --install-extension walkme.Meteor-extension-pack
code --install-extension xyz.local-history
code --install-extension yzhang.markdown-all-in-one
```

### vscode settings

You need a .vscode/settings.json that looks something like this....
{
    "python.pythonPath": "/usr/local/bin/python3",
    "python.testing.unittestArgs": [
        "-v",
        "-s",
        "./test",
        "-p",
        "*test*.py"
    ],
    "python.testing.pytestEnabled": true,
    "python.testing.nosetestsEnabled": false,
    "python.testing.unittestEnabled": false,
    "python.testing.pytestArgs": [
        "-s",
        "-v",
        "grib2_to_cb/test",
        "netcdf_to_cb/test",
        "ctc_to_cb/test"
    ],
    "python.linting.pylintEnabled": true,
    "python.linting.enabled": true
}

You also need a .vscode/launch.json that looks something like
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Current File",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal"
        },
        {
            "name": "Python: delta_models_hist",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/netcdf_to_cb/test/delta_hist.py",
            "cwd": "${workspaceFolder}/netcdf_to_cb",
            "args": [
                "-f", "~/model-mysql-cb-comp.txt"
            ],
            "console": "integratedTerminal",
        },
        {
            "name": "Python: delta_obs_hist",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/netcdf_to_cb/test/delta_hist.py",
            "cwd": "${workspaceFolder}/netcdf_to_cb",
            "args": [
                "-f", "~/obs-mysql-cb-comp.txt"
            ],
            "console": "integratedTerminal",
        },
        {
            "name": "Python: Current File",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}/netcdf_to_cb",
            "env": {
                "PYTHONPATH": "${cwd}"
            }
        },
        {
            "name": "Run gsd ingest netcdf metars V01",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/netcdf_to_cb/run_ingest_threads.py",
            "args": ["-s ${workspaceFolder}/netcdf_to_cb/test/load_spec_netcdf_metar_obs_V01.yaml",
                "-c ${env:HOME}/adb-cb1-credentials",
                "-p ${workspaceFolder}/netcdf_to_cb/test",
                "-m %Y%m%d_%H%M",
                "-o /tmp"
            ],
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}/netcdf_to_cb",
            "env": {
                "PYTHONPATH": "${cwd}"
            }
        },
        {
            "name": "Run gsd ingest grib files V01",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/grib2_to_cb/run_ingest_threads.py",
            "args": ["-s ${workspaceFolder}/grib2_to_cb/test/load_spec_grib_metar_hrrr_ops_V01.yaml",
                "-c ${env:HOME}/adb-cb1-credentials",
                "-p /opt/public/data/grids/hrrr/conus/wrfprs/grib2",
                "-m %y%j%H%f",
                "-o /opt/public/data/output/"
            ],
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}/grib2_to_cb",
            "env": {
                "PYTHONPATH": "${cwd}"
            }
        },
        {
            "name": "Run gsd ingest grib files V01 - test2",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/grib2_to_cb/run_ingest_threads.py",
            "args": ["-s /opt/data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml",
                "-c ${env:HOME}/adb-cb1-credentials",
                "-p /opt/data/grib2_to_cb/input_files",
                "-m %y%j%H%f",
                "-o /opt/public/data/output"
            ],
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}/grib2_to_cb",
            "env": {
                "PYTHONPATH": "${cwd}"
            }
        },
        {
            "name": "Debug Tests",
            "type": "python",
            "request": "test",
            "console": "integratedTerminal",
            "justMyCode": false
        }
    ]
}

### vscode interptreter

You need to choose the python interpreter. To do this do a cmd->Shift->P and scroll to
"select python interpreter" and choose the one for your test_venv.

## test execution

### command line

You can use the pytest module to invoke a test from the command line. For example...

```python3 -m pytest -s -v  /Users/randy.pierce/PycharmProjects/VXingest/netcdf_to_cb/test/test_int_metar_ctc.py::TestCTCBuilderV01::test_ctc_builder_hrrr_ops_all_hrrr_compare_model_obs_data```

would execute the integration test 'test_ctc_builder_hrrr_ops_all_hrrr_compare_model_obs_data' integration test method from the 'TestCTCBuilderV01' test class in the /Users/randy.pierce/PycharmProjects/VXingest/ctc_to_cb/test/test_metar_ctc.py python file and it would redirect the output of this integration test into a file 'test_result_ctc_compare.txt'.

The invocation ... ``` python3 -m pytest -s -v  /Users/randy.pierce/PycharmProjects/VXingest/netcdf_to_cb/test/test_unit_metar_obs_netcdf.py ```
would execute all of the unit tests in  '/Users/randy.pierce/PycharmProjects/VXingest/netcdf_to_cb/test/test_unit_metar_obs_netcdf.py' and give the test output on the command line.
Example:

``` sh
(test_env) pierce-lt:VXingest randy.pierce$ python3 -m pytest -s -v  /Users/randy.pierce/PycharmProjects/VXingest/netcdf_to_cb/test/test_unit_metar_obs_netcdf.py
====================================================================================== test session starts ======================================================================================
platform darwin -- Python 3.9.5, pytest-6.2.4, py-1.10.0, pluggy-0.13.1 -- /Users/randy.pierce/PycharmProjects/VXingest/test_env/bin/python3
cachedir: .pytest_cache
rootdir: /Users/randy.pierce/PycharmProjects/VXingest
collected 7 items
netcdf_to_cb/test/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test_build_load_job_doc PASSED
netcdf_to_cb/test/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test_cb_connect_disconnect PASSED
netcdf_to_cb/test/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test_credentials_and_load_spec PASSED
netcdf_to_cb/test/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test_derive_valid_time_epoch PASSED
netcdf_to_cb/test/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test_umask_value_transform PASSED
netcdf_to_cb/test/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test_vxingest_get_file_list PASSED
netcdf_to_cb/test/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test_write_load_job_to_files PASSED

======================================================================================= 7 passed in 5.39s =======================================================================================
(test_env) pierce-lt:VXingest randy.pierce$
```

### vscode testing

#### To use the vscode test utility click the little flask icon on the left panel.

The python test explorer will discover any tests that are in the code tree in a test directory with a name that starts with test_.

By clicking the arrow or the bug icon to the right of the test name you can either execute or debug the test within vscode. Clicking the 'bug' will cause the debugger to stop at any breakpoints.

You can also click the little page icon and that will open an editor on the individual test.

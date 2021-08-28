# mdtf-dagster-demo

Quick demo code to demonstrate how the tasks performed by the 
[MDTF-diagnostics](https://github.com/NOAA-GFDL/MDTF-diagnostics) package using
the [Dagster](https://dagster.io/) workflow engine. 

This is a proof-of-concept only.

## Installation

1. The code requires a pre-existing installation of the MDTF-diagnostics package,
    as well as its supporting data and [conda](https://docs.conda.io/en/latest/) environments. See installation instructions at the MDTF-diagnostics 
    [documentation site](https://mdtf-diagnostics.readthedocs.io/en/latest/sphinx/start_install.html).
    The demo can be run within the GFDL firewall by pointing the demo code to the pre-existing GFDL [site installation](https://mdtf-diagnostics.readthedocs.io/en/latest/sphinx_sites/NOAA_GFDL.html).

2. Installation of Dagster and the MDTF-diagnostics framework dependencies is 
    currently provided through a conda environment named `FRE-dagster-dev`, defined
    in `conda_env_dev.yml`. To install it, run
    ```
    > conda env create -f conda_env_dev.yml
    > conda activate FRE-dagster-dev
    ```

3. All demo code is in `mdtf_dagster_demo_1.py`. Replace `MDTF_ROOT` in this file
    with the path to the MDTF-diagnostics repo. 

## Running

The code in `mdtf_dagster_demo_1.py` defines a Dagster pipeline `mdtf_test` that can be invoked via the standard methods. For example, to start the [dagit](https://docs.dagster.io/concepts/dagit/dagit) web UI,

    ```
    > dagit -f mdtf_dagster_demo_1.py &
    ```
and open the returned URL in a browser. 

The external configuration required for this pipeline is provided in `demo_1_config.yml`. In particular, `input_file` should be the path to a configuration file for MDTF-diagnostics (`$MDTF_ROOT/src/default_tests.jsonc` can be used to run the diagnostics on included sample model data). 
This configuration can be passed to the pipeline via copy-and-pasting it into the dagit config editor, or by running the pipeline from the command line directly:
```
> dagster pipeline execute -f mdtf_dagster_demo_1.py -c demo_1_config.yml
```




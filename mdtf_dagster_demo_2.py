import os
import sys
import signal
MDTF_ROOT = '/Users/tsj/Documents/climate/MDTF-diagnostics'
sys.path.append(MDTF_ROOT)
# sys.argv = ['mdtf']

from src import core, diagnostic, cli, util, preprocessor
from . import mdtf_dagster_demo_1 as w1

import dagster as dg




@dg.solid(
    input_defs=[dg.InputDefinition(name="pv")],
    output_defs=[dg.OutputDefinition(name="var"), dg.OutputDefinition(name="preproc")]
)
def get_attrs(context, pv):
    yield dg.Output(pv.var, output_name="var")
    yield dg.Output(pv.pod.preprocessor, output_name="preproc")

@dg.solid(
    input_defs=[dg.InputDefinition(name="preproc"), dg.InputDefinition(name="var")],
    output_defs=[dg.OutputDefinition(name="ds")]
)
def load_ds(context, preproc, var):
    return preproc.load_ds(var)

@dg.solid(
    input_defs=[dg.InputDefinition(name="var"), dg.InputDefinition(name="ds")]
)
def crop_daterange(context, var, ds):
    func = preprocessor.CropDateRangeFunction(_,_)
    return func.process(var, ds)

@dg.solid(
    input_defs=[dg.InputDefinition(name="var"), dg.InputDefinition(name="ds")]
)
def precip_rate_to_flux(context, var, ds):
    func = preprocessor.PrecipRateToFluxFunction(_,_)
    return func.process(var, ds)

@dg.solid(
    input_defs=[dg.InputDefinition(name="var"), dg.InputDefinition(name="ds")]
)
def convert_units(context, var, ds):
    func = preprocessor.ConvertUnitsFunction(_,_)
    return func.process(var, ds)

@dg.solid(
    input_defs=[dg.InputDefinition(name="var"), dg.InputDefinition(name="ds")]
)
def extract_level(context, var, ds):
    func = preprocessor.ExtractLevelFunction(_,_)
    return func.process(var, ds)

@dg.solid(
    input_defs=[dg.InputDefinition(name="var"), dg.InputDefinition(name="ds")]
)
def rename_variables(context, var, ds):
    func = preprocessor.RenameVariablesFunction(_,_)
    return func.process(var, ds)

@dg.solid(
    input_defs=[
        dg.InputDefinition(name="preproc"),
        dg.InputDefinition(name="var"),
        dg.InputDefinition(name="ds")
    ]
)
def write_ds(context, preproc, var, ds):
    preproc.write_ds(var, ds)


# -------------------------------------------------------

@dg.composite_solid(
    input_defs=[dg.InputDefinition(name="pv")],
    output_defs=[dg.OutputDefinition(name="v")]
)
def data_preprocess(context, pv):
    pod, var = pv
    try:
        context.log.info(f"Preprocessing {var.name}.")

        var, preproc = get_attrs(pv)
        ds = load_ds(preproc, var)
        ds = crop_daterange(var, ds)
        ds = precip_rate_to_flux(var, ds)
        ds = convert_units(var, ds)
        ds = extract_level(var, ds)
        ds = rename_variables(var, ds)
        write_ds(preproc, var, ds)

        var.stage = diagnostic.VarlistEntryStage.PREPROCESSED
    except Exception as exc:
        context.log.exception(
            f"{util.exc_descriptor(exc)} while preprocessing {var.full_name}."
        )
        for d_key in var.iter_data_keys(status=core.ObjectStatus.ACTIVE):
            var.deactivate_data_key(d_key, exc)

        yield

# -------------------------------------------------------

@dg.pipeline
def mdtf_test():
    fmwk, case = w1.init_harness()

    # data query
    vars1 = w1.data_setup(case).map(w1.data_query)
    vars2 = w1.data_select(case, vars1.collect()).map(w1.data_fetch)
    vars3 = w1.data_preproc_setup(case, vars2.collect()).map(data_preprocess)
    case = w1.data_teardown(case, vars3.collect())

    # run the PODs
    run_mgr = w1.run_mgr_init(fmwk, case)
    pods1 = w1.run_setup(run_mgr).map(w1.run_pod)
    run_mgr = w1.run_teardown(run_mgr, pods1.collect())

    w1.cleanup_harness(fmwk, case, run_mgr)



import os
import sys
import signal
MDTF_ROOT = '/Users/tsj/Documents/climate/MDTF-diagnostics'
sys.path.append(MDTF_ROOT)
# sys.argv = ['mdtf']

from src import core, diagnostic, cli, util, data_sources, environment_manager

import dagster as dg

# solids:
#   init_harness:
#     config:
#       input_file: "/Users/tsj/Documents/climate/MDTF-diagnostics/temp_test.jsonc"
#       extra_flags: ["-v"]

# --------------------------------------

@dg.solid(
    output_defs=[
        dg.OutputDefinition(
            name="fmwk", dagster_type=core.MDTFFramework,
            description="Example documentation 1 for objects passed between solids."
        ),
        dg.OutputDefinition(
            name="case",
            description="Example documentation 2 for objects passed between solids."
        )
    ],
    config_schema={"input_file": str, 'extra_flags': list}
)
def init_harness(context):
    """Example documentation: the python function docstring is used by the GUI.
    """
    os.chdir(MDTF_ROOT)
    cli_argv = ' '.join(
        ['-f', context.solid_config['input_file']] + context.solid_config['extra_flags']
    )
    cli_obj = cli.MDTFTopLevelArgParser(MDTF_ROOT, argv=cli_argv)
    fmwk = cli_obj.dispatch()

    # unroll framework.main()
    fmwk.cases = dict(list(fmwk.cases.items())[0:1])
    new_d = dict()
    for case_name, case_d in fmwk.cases.items():
        context.log.info(f"### {fmwk.full_name}: initializing case '{case_name}'.")
        case = fmwk.DataSource(case_d, parent=fmwk)
        case.setup()
        new_d[case_name] = case
    fmwk.cases = new_d
    # util.transfer_log_cache(close=True)
    yield dg.Output(fmwk, output_name='fmwk')

    for case_name, case in fmwk.cases.items():
        if not case.failed:
            context.log.info(f"### {fmwk.full_name}: requesting data for case '{case_name}'.")

            ### splice in request_data()
            # Call cleanup method if we're killed
            signal.signal(signal.SIGTERM, case.query_and_fetch_cleanup)
            signal.signal(signal.SIGINT, case.query_and_fetch_cleanup)
            case.pre_query_and_fetch_hook()

            yield dg.Output(case, output_name='case')

@dg.solid(
    input_defs=[dg.InputDefinition(name="case")],
    output_defs=[dg.DynamicOutputDefinition(name="case_var")]
)
def data_setup(context, case):
    vars_to_query = [
        v for v in case.iter_vars_only(active=True) \
            if v.stage < diagnostic.VarlistEntryStage.QUERIED
    ]
    context.log.debug(f"Query batch: [{', '.join(v.full_name for v in vars_to_query)}].")
    case.pre_query_hook(vars_to_query)
    for v in vars_to_query:
        yield dg.DynamicOutput(value=(case, v), mapping_key=v.name, output_name="case_var")

@dg.solid(
    input_defs=[dg.InputDefinition(name="case_v")],
    output_defs=[dg.OutputDefinition(name="v")]
)
def data_query(context, case_v):
    case, vv = case_v

    context.log.info(vv.debug_str())
    def _data_query(v):
        try:
            context.log.info(f"Querying {v.translation}.")
            case.query_dataset(v) # sets v.data
            if not v.data:
                raise util.DataQueryEvent("No data found.", v)
            v.stage = diagnostic.VarlistEntryStage.QUERIED
            return v
        except util.DataQueryEvent as exc:
            v.deactivate(exc)
            raise exc
        except Exception as exc:
            chained_exc = util.chain_exc(exc,
                f"querying {v.translation} for {v.full_name}.",
                util.DataQueryEvent)
            v.deactivate(chained_exc)
            raise exc

    try:
        return _data_query(vv)
    except Exception as exc:
        for v_alt_set in vv.iter_alternates():
            for v_alt in v_alt_set:
                context.log.info(v_alt.full_name)
                try:
                    return _data_query(v_alt)
                except Exception:
                    continue
        raise exc

@dg.solid(
    input_defs=[dg.InputDefinition(name="case"),
        dg.InputDefinition(name="vars_list")],
    output_defs=[dg.DynamicOutputDefinition(name="case_var")]
)
def data_select(context, case, vars_list):
    case.post_query_hook(vars_list)
    case.set_experiment()

    vars_to_fetch = [
        v for v in case.iter_vars_only(active=True) \
            if v.stage < diagnostic.VarlistEntryStage.FETCHED
    ]
    context.log.debug(f"Fetch batch: [{', '.join(v.full_name for v in vars_to_fetch)}].")
    case.pre_fetch_hook(vars_to_fetch)
    for v in vars_to_fetch:
        yield dg.DynamicOutput(value=(case, v), mapping_key=v.name, output_name="case_var")

@dg.solid(
    input_defs=[dg.InputDefinition(name="case_v")],
    output_defs=[dg.OutputDefinition(name="v")]
)
def data_fetch(context, case_v):
    case, v = case_v
    try:
        context.log.info(f"Fetching {str(v)}.")
        # fetch on a per-DataKey basis
        for d_key in v.iter_data_keys(status=core.ObjectStatus.ACTIVE):
            try:
                if not case.is_fetch_necessary(d_key):
                    continue
                v.log.debug(f"Fetching {str(d_key)}.")
                case.fetch_dataset(v, d_key)
            except Exception as exc:
                d_key.deactivate(exc)
                break # no point continuing
        # check if var received everything
        for d_key in v.iter_data_keys(status=core.ObjectStatus.ACTIVE):
            if not d_key.local_data:
                raise util.DataFetchEvent("Fetch failed.", d_key)
        v.stage = diagnostic.VarlistEntryStage.FETCHED
    except Exception as exc:
        chained_exc = util.chain_exc(exc,
            f"fetching data for {v.full_name}.",
            util.DataFetchEvent)
        v.deactivate(chained_exc)
    return v

@dg.solid(
    input_defs=[dg.InputDefinition(name="case"),
        dg.InputDefinition(name="vars_list")],
    output_defs=[dg.DynamicOutputDefinition(name="pod_var")]
)
def data_preproc_setup(context, case, vars_list):
    case.post_fetch_hook(vars_list)
    for pod in case.iter_children(status=core.ObjectStatus.ACTIVE):
        pod.preprocessor.setup(case, pod)

    for pv in case.iter_vars(active=True):
        if pv.var.stage < diagnostic.VarlistEntryStage.PREPROCESSED:
            yield dg.DynamicOutput(value=(pv.pod, pv.var), output_name="pod_var",
                mapping_key=f"{pv.pod.name}_{pv.var.name}")

@dg.solid(
    input_defs=[dg.InputDefinition(name="pod_var")],
    output_defs=[dg.OutputDefinition(name="v")]
)
def data_preprocess(context, pod_var):
    pod, var = pod_var
    try:
        context.log.info(f"Preprocessing {var.name}.")
        pod.preprocessor.process(var)
        var.stage = diagnostic.VarlistEntryStage.PREPROCESSED
    except Exception as exc:
        context.log.exception(
            f"{util.exc_descriptor(exc)} while preprocessing {var.full_name}."
        )
        for d_key in var.iter_data_keys(status=core.ObjectStatus.ACTIVE):
            var.deactivate_data_key(d_key, exc)

@dg.solid(
    input_defs=[dg.InputDefinition(name="case"),
        dg.InputDefinition(name="vars_list")],
    output_defs=[dg.OutputDefinition(name="case")]
)
def data_teardown(context, case, vars_list):
    # clean up regardless of success/fail
    case.post_query_and_fetch_hook()
    for p in case.iter_children():
        for v in p.iter_children():
            if v.status == core.ObjectStatus.ACTIVE:
                context.log.debug(f'Data request for {v.full_name} completed succesfully.')
                v.status = core.ObjectStatus.SUCCEEDED
            elif v.failed:
                context.log.debug(f'Data request for {v.full_name} failed.')
            else:
                context.log.debug(f'Data request for {v.full_name} not used.')
        if p.failed:
            p.log.debug(f'Data request for {p.full_name} failed.')
        else:
            p.log.debug(f'Data request for {p.full_name} completed succesfully.')
    return case

# @dg.composite_solid(
#     input_defs=[dg.InputDefinition(name="case")],
#     output_defs=[dg.OutputDefinition(name="case")]
# )
# def data_stage(context, case):
#     # data query
#     vars1 = data_setup(case).map(data_query)
#     vars2 = data_select(case, vars1.collect()).map(data_fetch)
#     vars3 = data_preproc_setup(case, vars2.collect()).map(data_preprocess)
#     case = data_teardown(case, vars3.collect())
#     return case

# -------------------------------------------------------

@dg.solid(
    input_defs=[dg.InputDefinition(name="fmwk"), dg.InputDefinition(name="case")],
    output_defs=[dg.OutputDefinition(name="run_mgr")]
)
def run_mgr_init(context, fmwk, case):
    run_mgr = fmwk.RuntimeManager(case, fmwk.EnvironmentManager)
    run_mgr.setup()
    return run_mgr

@dg.solid(
    input_defs=[dg.InputDefinition(name="run_mgr")],
    output_defs=[dg.DynamicOutputDefinition(name="rm_pod")]
)
def run_setup(context, run_mgr):
    # Call cleanup method if we're killed
    signal.signal(signal.SIGTERM, run_mgr.runtime_terminate)
    signal.signal(signal.SIGINT, run_mgr.runtime_terminate)
    test_list = [p for p in run_mgr.iter_active_pods()]
    if not test_list:
        context.log.error('no PODs met data requirements; returning')
        return

    for p in run_mgr.iter_active_pods():
        yield dg.DynamicOutput(value=(run_mgr, p), output_name="rm_pod",
            mapping_key=p.pod.name)

@dg.solid(
    input_defs=[dg.InputDefinition(name="rm_pod")],
    output_defs=[dg.OutputDefinition(name="p")]
)
def run_pod(context, rm_pod):
    run_mgr, p = rm_pod
    env_vars_base = os.environ.copy()
    context.log.info(f'run {p.pod.full_name}.')
    try:
        p.pre_run_setup()
    except Exception as exc:
        p.setup_exception_handler(exc)
        raise
    try:
        p.pod.log_file.write(f"### Start execution of {p.pod.full_name}\n")
        p.pod.log_file.write(80 * '-' + '\n')
        p.pod.log_file.flush()
        p.process = run_mgr.spawn_subprocess(p, env_vars_base)
        if p.process is not None:
            p.process.wait()
    except Exception as exc:
        p.runtime_exception_handler(exc)
        raise
    return p

@dg.solid(
    input_defs=[dg.InputDefinition(name="run_mgr"), dg.InputDefinition(name="pods_list")],
    output_defs=[dg.OutputDefinition(name="run_mgr")]
)
def run_teardown(context, run_mgr, pods_list):
    for p in pods_list:
        # if p.process is not None:
        #     p.process.wait()
        p.tear_down()
    context.log.info('completed all PODs.')
    run_mgr.tear_down()
    return run_mgr

# @dg.composite_solid(
#     input_defs=[dg.InputDefinition(name="fmwk"), dg.InputDefinition(name="case")],
#     output_defs=[dg.OutputDefinition(name="run_mgr")]
# )
# def run_stage(context, fmwk, case):


# -------------------------------------------------------
@dg.solid(
    input_defs=[dg.InputDefinition(name="fmwk"), dg.InputDefinition(name="case"),
        dg.InputDefinition(name="run_mgr")]
)
def cleanup_harness(context, fmwk, case, run_mgr):
    out_mgr = fmwk.OutputManager(case)
    out_mgr.make_output()

    tempdirs = core.TempDirManager()
    tempdirs.cleanup()
    core.print_summary(fmwk)

# -------------------------------------------------------

@dg.pipeline
def mdtf_test():
    fmwk, case = init_harness()

    # case = data_stage(case)
    # data query
    vars1 = data_setup(case).map(data_query)
    vars2 = data_select(case, vars1.collect()).map(data_fetch)
    vars3 = data_preproc_setup(case, vars2.collect()).map(data_preprocess)
    case = data_teardown(case, vars3.collect())

    #run_mgr = run_stage(fmwk, case)
    # run the PODs
    run_mgr = run_mgr_init(fmwk, case)
    pods1 = run_setup(run_mgr).map(run_pod)
    run_mgr = run_teardown(run_mgr, pods1.collect())

    cleanup_harness(fmwk, case, run_mgr)



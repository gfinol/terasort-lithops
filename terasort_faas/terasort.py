import os
import click
from lithops import FunctionExecutor, Storage
from datetime import datetime
import time
from terasort_faas.IO import get_data_size
from terasort_faas.aux import remove_intermediates, warm_up_functions
from terasort_faas.cost_reporter import lithops_cost, s3_direct_shuffle_cost
from terasort_faas.logging.logging import setup_logger
from terasort_faas.logging.results import result_summary, compute_stats
from terasort_faas.mapper import Mapper, run_mapper
from terasort_faas.reducer import Reducer, run_reducer
from terasort_faas.config import bcolors
import logging
from terasort_faas.config import *
# import yaml
import cloudpickle as pickle

console_logger = logging.getLogger(CONSOLE_LOGGER)
execution_logger = logging.getLogger(EXECUTION_LOGGER)
lithops_logger = logging.getLogger(__name__)

def run_terasort(
        bucket,
        key,
        map_parallelism,
        reduce_parallelism,
        runtime_name,
        runtime_memory,
        warm_up=False
):
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    timestamp_prefix = f"{timestamp}"
    setup_logger(timestamp_prefix)
    
    executor = FunctionExecutor(runtime_memory=runtime_memory, runtime=runtime_name)

    if warm_up:
        warm_up_functions(runtime_name, runtime_memory, executor=executor, n=map_parallelism+reduce_parallelism)

    dataset_size = get_data_size(executor.storage, bucket, key)

    click.echo("Sorting dataset: "+bcolors.BOLD+bcolors.OKBLUE+"%s "%(key)+bcolors.ENDC+bcolors.ENDC+"(%dMB)"%(dataset_size / 1024 / 1024))
    click.echo("\t- "+bcolors.BOLD+"%d"%(map_parallelism)+bcolors.ENDC+" map partitions.")
    click.echo("\t- "+bcolors.BOLD+"%d"%(reduce_parallelism)+bcolors.ENDC+" reduce partitions.")

    execution_logs = {
        "execution_info": {
                "dataset": key,
                "map_parallelism": map_parallelism,
                "reduce_parallelism": reduce_parallelism,
                "dataset_size": dataset_size / 1024 / 1024,
                "timestamp": timestamp_prefix,
                "runtime": runtime_name,
                "runtime_memory": runtime_memory
            }
    }

    mappers = [
        Mapper(
            partition_id,
            map_parallelism,
            reduce_parallelism,
            timestamp_prefix,
            bucket,
            key
        )
        for partition_id in range(map_parallelism)
    ]

    start_time = time.time()
    # run_mappers
    map_futures = executor.map(run_mapper, mappers)

    boto = Storage().get_client()
    multipart_response = boto.create_multipart_upload(Bucket=bucket, Key=f"{key}_sorted")
    upload_id = multipart_response['UploadId']

    reducers = [
        Reducer(
            partition_id,
            map_parallelism,
            reduce_parallelism,
            timestamp_prefix,
            bucket,
            key,
            upload_id
        )
        for partition_id in range(reduce_parallelism)
    ]

    executor.wait(map_futures, return_when=20)

    # run_reducers
    reducer_futures = executor.map(run_reducer, reducers)
    executor.wait(reducer_futures)
    end_time = time.time()

    click.echo(bcolors.OKGREEN+bcolors.BOLD+"Client sort time: %.2f s"%(end_time-start_time)+bcolors.ENDC+bcolors.ENDC)

    reducers_results = executor.get_result(reducer_futures)

    parts = [e['part'] for e in reducers_results]

    mappers_results = executor.get_result(map_futures)

    # Complete multipart upload
    function_results = mappers_results + reducers_results
    boto.complete_multipart_upload(Bucket=bucket, Key=f'{key}_sorted', UploadId=upload_id,
                                     MultipartUpload={'Parts': parts})

    for result in function_results:
        for k, v in result.items():
            execution_logs[k] = v
        # execution_logger.info(yaml.dump(
        #         result, 
        #         default_flow_style=False
        #     ))

    execution_logs["map_data"] = [
        {'stats': future.stats, 'result': res, 'runtime_memory': future.runtime_memory}
        for future, res in zip(map_futures, mappers_results)
    ]
    execution_logs["red_data"] = [
        {'stats': future.stats, 'result': res, 'runtime_memory': future.runtime_memory}
        for future, res in zip(reducer_futures, reducers_results)
    ]
    execution_data = {
        "start_time": start_time,
        "end_time": end_time
    }
    execution_logs["compression"] = True
    execution_logs["sort"] = execution_data

    map_cost = lithops_cost(map_futures, runtime_memory)
    red_cost = lithops_cost(reducer_futures, runtime_memory)
    l_cost = {'total_cost': map_cost['total_cost'] + red_cost['total_cost'],
                'map_cost': map_cost,
                'red_cost': red_cost}
    shuffle_cost = s3_direct_shuffle_cost(map_parallelism, reduce_parallelism)
    cost = {'total_cost': l_cost['total_cost'] + shuffle_cost,
            'lithops_cost': l_cost,
            'shuffle_cost': shuffle_cost}
    execution_logs['cost'] = cost

    executor.plot(fs=map_futures+reducer_futures, dst=f'{LOG_PATH}{timestamp_prefix}')

    execution_logs['execution_results'] = compute_stats(execution_logs)
    log_file = os.path.join(LOG_PATH, "%s.pickle"%(timestamp_prefix))
    pickle.dump(execution_logs, open(log_file, "wb"))

    print("Log file: %s"%(log_file))

    click.echo("\n\nRemoving intermediates...")
    remove_intermediates(executor, bucket, timestamp_prefix)

    result_summary(log_file)
    
import math

### Lambda Pricing ###
# $0.0000166667 for every GB-second
LAMBDA_PRICE_GB_SEC = 0.0000166667
LAMBDA_PRICE_MB_SEC = LAMBDA_PRICE_GB_SEC / 1024

# $0.20 per 1M requests
LAMBDA_PRICE_PER_REQUEST = 0.20 / 1_000_000

### S3 Pricing ###
# $0.005 per PUT, COPY, POST, LIST requests (per 1,000 requests)
S3_PRICE_WRITE_REQ = 0.005 / 1000

# $0.0004 per GET, SELECT and all other requests (per 1,000 requests)
S3_PRICE_READ_REQ = 0.0004 / 1000



# Computing cost of running a lithops job
def lithops_cost(futures, runtime_memory_mb: int) -> dict[str, float]:
    total_billed_time = 0
    execution_cost = 0
    invocation_cost = 0
    s3_requests_cost = 0

    for future in futures:
        stats = future.stats
        exec_time = stats['worker_exec_time']
        execution_cost += exec_time * LAMBDA_PRICE_MB_SEC * runtime_memory_mb
        invocation_cost += LAMBDA_PRICE_PER_REQUEST
        s3_requests_cost += stats['host_status_query_count'] * S3_PRICE_READ_REQ
        s3_requests_cost += stats['host_result_query_count'] * S3_PRICE_READ_REQ

        # Function writes to S3 the result once
        s3_requests_cost += S3_PRICE_WRITE_REQ

        total_billed_time += exec_time

    total_cost = execution_cost + invocation_cost + s3_requests_cost
    return {'total_billed_time': total_billed_time,
            'execution_cost': execution_cost,
            'invocation_cost': invocation_cost,
            's3_requests_cost': s3_requests_cost,
            'total_cost': total_cost}



def s3_direct_shuffle_cost(mappers: int, reducers: int) -> float:
    write_cost = (mappers * reducers) * S3_PRICE_WRITE_REQ
    read_cost = (mappers * reducers) * S3_PRICE_READ_REQ
    return write_cost + read_cost


def s3_two_level_shuffle_cost(workers: int) -> float:
    write_cost = workers * math.sqrt(workers) * S3_PRICE_WRITE_REQ
    read_cost = workers * math.sqrt(workers) * S3_PRICE_READ_REQ
    return write_cost + read_cost


def read_dataset_cost(workers: int) -> float:
    return workers * S3_PRICE_READ_REQ


def write_dataset_cost(workers: int) -> float:
    return workers * S3_PRICE_WRITE_REQ

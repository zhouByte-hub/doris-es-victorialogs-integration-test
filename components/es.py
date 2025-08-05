import time
import threading
import psutil
import json
import sys
from elasticsearch import Elasticsearch
from typing import List, Dict, Any

# 配置
ES_HOST = "http://127.0.0.1:9200"
ES_INDEX = "logs"

es_client = Elasticsearch([ES_HOST], timeout=60)

def write_worker(worker_id: int, batch_size: int, duration: int, result_queue: List[Dict]):
    start_time = time.time()
    errors = 0
    total_latency = 0
    success_count = 0

    while time.time() - start_time < duration:
        bulk_data = []
        for i in range(batch_size):
            # 生成与Doris相同结构的日志数据
            log_id = f"log_{worker_id}_{success_count + i}"
            project = f"project_{worker_id % 5}"  # 模拟5个不同项目
            severity = ["INFO", "WARNING", "ERROR", "DEBUG"][i % 4]  # 模拟不同严重级别
            event = f"event_{i % 10}"  # 模拟10种不同事件
            trigger_dt = time.strftime('%Y-%m-%dT%H:%M:%SZ')  # ES标准时间格式
            content = f"Sample log content for {event} with severity {severity}"
            trace = f"trace_{worker_id}_{success_count + i}"
            span = f"span_{worker_id}_{success_count + i}"
            create_dt = time.strftime('%Y-%m-%dT%H:%M:%SZ')  # ES标准时间格式
            
            doc = {
                "logs_id": log_id,
                "project_name": project,
                "serverity_text": severity,
                "event_name": event,
                "trigger_time": trigger_dt,
                "content": content,
                "trace_id": trace,
                "span_id": span,
                "create_time": create_dt
            }
            bulk_data.append({"index": {"_index": ES_INDEX}})
            bulk_data.append(doc)

        try:
            start = time.time()
            es_client.bulk(operations=bulk_data, refresh=False, timeout=60)
            latency = time.time() - start
            total_latency += latency
            success_count += batch_size
        except Exception as e:
            print(f"[ES Writer-{worker_id}] Error: {e}")
            errors += 1

    avg_latency = total_latency / success_count if success_count > 0 else 0
    success_rate = success_count / (success_count + errors) if (success_count + errors) > 0 else 0

    result_queue.append({
        "component": "Elasticsearch",
        "test_type": "write",
        "worker_id": worker_id,
        "success_count": success_count,
        "errors": errors,
        "avg_latency": avg_latency,
        "total_time": duration,
        "success_rate": success_rate
    })

## 范围查询：   {"query":{"range":{"triggerTime":{"gte":"开始时间","lte":"结束时间"}}},"from":0,"size":100}
## 聚合查询：   {"size":0,"aggs":{"log_levels":{"terms":{"field":"serverity_text.keyword"}}}}
## 多条件查询： {"query":{"bool":{"must":[{"match":{"project_name":"project_2"}},{"match":{"serverity_text":"INFO"}},{"range":{"trigger_time":{"gte":"开始时间","lte":"结束时间"}}}]}},"size":100}
def query_worker(worker_id: int, duration: int, result_queue: List[Dict]):
    start_time = time.time()
    response_times = []
    while time.time() - start_time < duration:
        try:
            query = {"size":0,"aggs":{"log_levels":{"terms":{"field":"serverity_text.keyword"}}}}
            t1 = time.time()
            es_client.search(index=ES_INDEX, body=query, timeout=60)
            response_times.append(time.time() - t1)
        except Exception as e:
            print(f"[ES Query-{worker_id}] Error: {e}")

    avg_resp = sum(response_times) / len(response_times) if response_times else 0
    result_queue.append({
        "component": "Elasticsearch",
        "test_type": "query",
        "worker_id": worker_id,
        "avg_response_time": avg_resp,
        "query_count": len(response_times)
    })

def monitor_resources(result_dict: Dict[str, Any], duration: int):
    process = psutil.Process()
    cpu_samples = []
    mem_samples = []
    start_time = time.time()
    while time.time() - start_time < duration:
        try:
            cpu_samples.append(process.cpu_percent(interval=None))
            mem_samples.append(process.memory_info().rss / 1024 / 1024)
            time.sleep(0.5)
        except:
            break
    result_dict['cpu_percent_avg'] = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
    result_dict['cpu_percent_peak'] = max(cpu_samples) if cpu_samples else 0
    result_dict['memory_mb_avg'] = sum(mem_samples) / len(mem_samples) if mem_samples else 0
    result_dict['memory_mb_peak'] = max(mem_samples) if mem_samples else 0

def run_write_test(concurrency: int = 4, batch_size: int = 100, duration: int = 1):
    result_queue = []
    resource_result = {}
    threads = []

    monitor_thread = threading.Thread(target=monitor_resources, args=(resource_result, duration))
    monitor_thread.start()

    for i in range(concurrency):
        t = threading.Thread(target=write_worker, args=(i, batch_size, duration, result_queue))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    monitor_thread.join()

    total_success = sum(r['success_count'] for r in result_queue)
    total_errors = sum(r['errors'] for r in result_queue)
    avg_latency = sum(r['avg_latency'] * r['success_count'] for r in result_queue) / total_success if total_success > 0 else 0
    success_rate = total_success / (total_success + total_errors) if (total_success + total_errors) > 0 else 0

    return {
        "component": "Elasticsearch",
        "test_type": "write",
        "success_rate": success_rate,
        "avg_latency_ms": avg_latency * 1000,
        "error_count": total_errors,
        "total_time_s": duration,
        "avg_cpu_cores": resource_result['cpu_percent_avg'] / 100,
        "peak_cpu_cores": resource_result['cpu_percent_peak'] / 100,
        "avg_memory_mb": resource_result['memory_mb_avg'],
        "peak_memory_mb": resource_result['memory_mb_peak'],
        "concurrency": concurrency,
        "batch_size": batch_size,
        "duration_s": f"{duration} seconds"
    }

def run_query_test(concurrency: int = 4, duration: int = 1):
    result_queue = []
    resource_result = {}
    threads = []

    monitor_thread = threading.Thread(target=monitor_resources, args=(resource_result, duration))
    monitor_thread.start()

    for i in range(concurrency):
        t = threading.Thread(target=query_worker, args=(i, duration, result_queue))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    monitor_thread.join()

    avg_response_time = sum(r['avg_response_time'] for r in result_queue) / len(result_queue) if result_queue else 0

    return {
        "component": "Elasticsearch",
        "test_type": "query",
        "avg_response_time_ms": avg_response_time * 1000,
        "avg_cpu_cores": resource_result['cpu_percent_avg'] / 100,
        "peak_cpu_cores": resource_result['cpu_percent_peak'] / 100,
        "avg_memory_mb": resource_result['memory_mb_avg'],
        "peak_memory_mb": resource_result['memory_mb_peak'],
        "concurrency": concurrency,
        "duration": f"{duration} seconds"
    }


## sys.args [type, concurrency, batch_size, duration]
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python doris.py <concurrency> <duration> <test_type>")
        sys.exit(1)

    if sys.argv[1] == "write":
        result = run_write_test(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))
        print(json.dumps(result))
    elif sys.argv[1] == "query":
        result = run_query_test(int(sys.argv[2]), int(sys.argv[4]))
        print(json.dumps(result))
    else:
        print("Invalid test type. Please choose 'write' or 'query'.")
        sys.exit(1)
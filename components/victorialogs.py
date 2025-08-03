import time
import threading
import psutil
import json
import sys
import requests
from typing import List, Dict, Any
import random
import string
from datetime import datetime, timezone, timedelta
import uuid

PROJECTS = ["otlp_service_A", "otlp_service_B", "api-gateway", "auth-service", "payment-service"]
SEVERITIES = ["INFO", "WARN", "ERROR", "DEBUG"]
EVENTS = ["LoginAttempt", "PaymentFailed", "ServerError", "HealthCheck", "DataExport"]

def random_text(length=100):
    return ''.join(random.choices(string.ascii_letters + string.digits + " .,!", k=length))


# curl -X POST   -H 'Content-Type: application/stream+json'   --data-binary $'{"project_name":"otlp_service_A","serverity_text":"INFO","event_name":null,"_time":"2025-08-03T15:32:10Z","_msg":"Creating log with ID: 051b7e63-42bf-4268-a2af-598639a3199a","trace_id":"c38aad4833385782ca261b6048f77d1f","span_id":"6272fe7d9b2d8d39","diff":1,"time":1753854643032}' 'http://43.139.97.119:9428/insert/jsonline'
def write_worker(worker_id: int, batch_size: int, duration: int, result_queue: List[Dict]):
    start_time = time.time()
    errors = 0
    total_latency = 0
    success_count = 0

    headers = {
        "Content-Type": "application/stream+json"
    }

    while time.time() - start_time < duration:
        try:
            # 构建 data-binary 内容：每行一个 JSON
            lines = []
            now = datetime.now(timezone.utc)  # 使用 UTC 时间
            for _ in range(batch_size):
                log_id = str(uuid.uuid4())
                project = random.choice(PROJECTS)
                severity = random.choice(SEVERITIES)
                event = random.choice(EVENTS) if random.random() < 0.7 else None
                content = f"Log event occurred: {random_text(60)}"
                trace_id = str(uuid.uuid4()).replace("-", "")  # 32位小写hex
                span_id = str(uuid.uuid4()).replace("-", "")[:16]
                # 触发时间：最近1小时内
                trigger_dt = now - timedelta(seconds=random.randint(0, 3600))
                # 时间戳（毫秒）
                timestamp_ms = int(trigger_dt.timestamp() * 1000)

                # 构造单条日志（VictoriaLogs jsonline 格式）
                log_entry = {
                    "logs_id": log_id,
                    "project_name": project,
                    "serverity_text": severity,
                    "event_name": event,
                    "_time": trigger_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),  # ISO8601 UTC
                    "_msg": content,
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "time": timestamp_ms,  # 自定义时间戳字段（毫秒）
                    "diff": random.randint(1, 100)  # 可选字段，用于测试
                }
                lines.append(json.dumps(log_entry, ensure_ascii=False))

            # 多行 JSON：每行一个对象
            payload = "\n".join(lines) + "\n"

            start_write = time.time()
            response = requests.post(
                "http://127.0.0.1:9428/insert/jsonline",
                data=payload.encode('utf-8'),
                headers=headers,
                timeout=60000
            )
            latency = time.time() - start_write

            if response.status_code in (200, 204):
                total_latency += latency
                success_count += len(lines)
            else:
                errors += len(lines)
                print(f"[VL-{worker_id}] HTTP {response.status_code}: {response.text}")

        except Exception as e:
            print(f"[VL-{worker_id}] Error: {e}")
            errors += 1

    avg_latency = total_latency / success_count if success_count > 0 else 0
    success_rate = success_count / (success_count + errors) if (success_count + errors) > 0 else 0

    result_queue.append({
        "component": "VictoriaLogs",
        "test_type": "write",
        "worker_id": worker_id,
        "success_count": success_count,
        "errors": errors,
        "avg_latency": avg_latency,
        "total_time": duration,
        "success_rate": success_rate
    })

## curl http://127.0.0.1:9428/select/logsql/query -d 'query=log' -d 'limit=100'
## 范围查询：   
## 聚合查询：   
## 条件查询：

def query_worker(worker_id: int, duration: int, result_queue: List[Dict]):
    query_url = "http://127.0.0.1:9428/select/logsql/query"
    
    query_data = {
        'query': 'log',
        'limit': 100
    }
    
    start_time = time.time()
    response_times = []
    error_count = 0

    while time.time() - start_time < duration:
        try:
            t1 = time.time()
            resp = requests.post(query_url, data=query_data, timeout=60000)
            resp.raise_for_status()  # 检查 HTTP 错误状态码
            response_time = time.time() - t1
            response_times.append(response_time)
        except Exception as e:
            error_count += 1
            print(f"[VictoriaLogs Query-{worker_id}] Error: {e}")

    avg_resp = sum(response_times) / len(response_times) if response_times else 0
    result_queue.append({
        "component": "VictoriaLogs",
        "test_type": "query",
        "worker_id": worker_id,
        "avg_response_time": round(avg_resp, 3),
        "query_count": len(response_times),
        "errors": error_count,
        "total_duration": duration
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
        "component": "VictoriaLogs",
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
        "component": "VictoriaLogs",
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
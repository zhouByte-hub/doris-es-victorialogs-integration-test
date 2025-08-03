# doris_test.py
import time
import pymysql
import threading
import psutil
import json
from typing import List, Dict, Any
import pandas as pd
import sys
import random
import uuid
import string

# 配置参数
DORIS_HOST = "127.0.0.1"
DORIS_PORT = 9030
DORIS_USER = "root"
DORIS_PASSWORD = ""
DORIS_DB = "test"
DORIS_TABLE = "logs"

def random_text(length=100):
    return ''.join(random.choices(string.ascii_letters + string.digits + " .,!", k=length))

def write_worker(worker_id: int, batch_size: int, duration: int, result_queue: List[Dict]):
    start_time = time.time()
    errors = 0
    total_latency = 0
    success_count = 0
    conn = pymysql.connect(host=DORIS_HOST, port=DORIS_PORT, user=DORIS_USER, password=DORIS_PASSWORD, database=DORIS_DB)
    cursor = conn.cursor()

    # 可选的枚举值
    severities = ["INFO", "WARN", "ERROR", "DEBUG"]
    projects = ["project-a", "project-b", "project-c", "monitoring-svc", "api-gateway"]
    events = ["LoginAttempt", "PaymentFailed", "ServerError", "HealthCheck", "DataExport"]

    while time.time() - start_time < duration:
        try:
            # 生成 batch_size 条符合 logs 表结构的数据
            data_batch = []
            now = pd.Timestamp.now()
            for _ in range(batch_size):
                log_id = str(uuid.uuid4())[:32]  # VARCHAR(128)，用 UUID 截取
                project = random.choice(projects)
                severity = random.choice(severities)
                event = random.choice(events)
                trigger_t = now - pd.Timedelta(seconds=random.randint(0, 3600))  # 最近一小时
                content = random_text(200)  # TEXT 字段
                trace_id = str(uuid.uuid4()) if random.random() < 0.8 else "\\N"  # 80% 有 trace_id
                span_id = str(uuid.uuid4())[:16] if trace_id else "\\N"
                create_t = now

                # 注意：Doris 中如果允许 NULL，可以用 None，否则用 "\\N" 表示空字符串或 NULL（取决于表定义）
                data_batch.append((
                    log_id,
                    project,
                    severity,
                    event,
                    trigger_t.strftime('%Y-%m-%d %H:%M:%S'),
                    content,
                    trace_id,
                    span_id,
                    create_t.strftime('%Y-%m-%d %H:%M:%S')
                ))

            insert_sql = f"""
            INSERT INTO {DORIS_TABLE} (
                logs_id, project_name, serverity_text, event_name, trigger_time,
                content, trace_id, span_id, create_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            start_write = time.time()
            cursor.executemany(insert_sql, data_batch)
            conn.commit()
            latency = time.time() - start_write
            total_latency += latency
            success_count += len(data_batch)

        except Exception as e:
            print(f"[Doris Writer-{worker_id}] Error: {e}")
            errors += 1

    cursor.close()
    conn.close()

    avg_latency = total_latency / success_count if success_count > 0 else 0
    success_rate = success_count / (success_count + errors) if (success_count + errors) > 0 else 0

    result_queue.append({
        "component": "Doris",
        "test_type": "write",
        "worker_id": worker_id,
        "success_count": success_count,
        "errors": errors,
        "avg_latency": avg_latency,
        "total_time": duration,
        "success_rate": success_rate
    })


## 范围查询:    SELECT * FROM test.logs WHERE trigger_time BETWEEN '开始时间' AND '结束时间' LIMIT 条数; 
## 聚合查询：   SELECT serverity_text AS log_level,  COUNT(*) AS log_count FROM test.logs GROUP BY serverity_text;
## 多条件查询： SELECT * FROM test.logs WHERE serverity_text = 'ERROR' AND (trigger_time BETWEEN '开始时间' AND '结束时间') AND project_name = 'project-a' LIMIT 条数;
def query_worker(worker_id: int, duration: int, result_queue: List[Dict]):
    conn = pymysql.connect(host=DORIS_HOST, port=DORIS_PORT, user=DORIS_USER, password=DORIS_PASSWORD, database=DORIS_DB)
    cursor = conn.cursor()
    start_time = time.time()
    response_times = []

    while time.time() - start_time < duration:
        try:
            query = f"SELECT serverity_text AS log_level,  COUNT(*) AS log_count FROM {DORIS_TABLE} GROUP BY serverity_text;" ## {DORIS_TABLE}
            t1 = time.time()
            cursor.execute(query)
            cursor.fetchall()
            response_times.append(time.time() - t1)
        except Exception as e:
            print(f"[Doris Query-{worker_id}] Error: {e}")

    cursor.close()
    conn.close()

    avg_resp = sum(response_times) / len(response_times) if response_times else 0
    result_queue.append({
        "component": "Doris",
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
            mem_samples.append(process.memory_info().rss / 1024 / 1024)  # MB
            time.sleep(0.5)
        except:
            break

    result_dict['cpu_percent_avg'] = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
    result_dict['cpu_percent_peak'] = max(cpu_samples) if cpu_samples else 0
    result_dict['memory_mb_avg'] = sum(mem_samples) / len(mem_samples) if mem_samples else 0
    result_dict['memory_mb_peak'] = max(mem_samples) if mem_samples else 0

def run_write_test(concurrency: int = 4, batch_size: int = 1000, duration: int = 1):
    result_queue = []
    resource_result = {}
    threads = []

    # Start monitoring
    monitor_thread = threading.Thread(target=monitor_resources, args=(resource_result, duration))
    monitor_thread.start()

    # Start writers
    for i in range(concurrency):
        t = threading.Thread(target=write_worker, args=(i, batch_size, duration, result_queue))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    monitor_thread.join()

    # Aggregate results
    total_success = sum(r['success_count'] for r in result_queue)
    total_errors = sum(r['errors'] for r in result_queue)
    total_time = duration
    avg_latency = sum(r['avg_latency'] * r['success_count'] for r in result_queue) / total_success if total_success > 0 else 0
    success_rate = total_success / (total_success + total_errors) if (total_success + total_errors) > 0 else 0

    return {
        "component": "Doris",
        "test_type": "write",
        "success_rate": success_rate,
        "avg_latency_ms": avg_latency * 1000,
        "error_count": total_errors,
        "total_time_s": total_time,
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
        "component": "Doris",
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
import subprocess
import json
import pandas as pd
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

CONFIG = {
    "type": "query",
    "concurrency": "4",
    "batch_size": "100",
    "duration": "10",
}

def run_test(script_name: str, test_type: str):
    ## sys.args [type, concurrency, batch_size, duration]
    cmd = [sys.executable, script_name, test_type, CONFIG["concurrency"], CONFIG["batch_size"], CONFIG["duration"]]
    print(f"Running {script_name} for {test_type} test...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error in {script_name}: {result.stderr}")
        return {}
    try:
        return json.loads(result.stdout.strip())
    except Exception as e:
        print(f"Parse error: {e}")
        print(f"Output: {result.stdout}")
        return {}

def main():
    results = []
    scripts = ["components/doris.py", "components/es.py", "components/victorialogs.py"]
    
    print(f"Starting parallel execution of {len(scripts)} tests...")
    
    # 使用线程池并行执行测试
    with ThreadPoolExecutor(max_workers=len(scripts)) as executor:
        # 提交所有任务
        future_to_script = {
            executor.submit(run_test, script, CONFIG["type"]): script 
            for script in scripts
        }
        
        # 收集结果
        for future in as_completed(future_to_script):
            script = future_to_script[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    print(f"✅ {script} completed successfully")
                else:
                    print(f"❌ {script} failed or returned empty result")
            except Exception as e:
                print(f"❌ {script} raised an exception: {e}")

    # 保存为 CSV
    if results:
        df = pd.DataFrame(results)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"test_{CONFIG['type']}_{timestamp}.csv"
        df.to_csv(filename, index=False)
        print(f"\n✅ Results saved to {filename}")
        print(df)
    else:
        print("❌ No results to save - all tests failed")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
import sys
import json
import urllib.request
import urllib.parse

def query_container(prom_url, pod, container, duration):
    query = f'max_over_time(container_memory_working_set_bytes{{namespace="default",pod="{pod}",container="{container}"}}[{duration}s])'
    params = {'query': query}
    url = f"{prom_url}?{urllib.parse.urlencode(params)}"

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())

        if data['status'] == 'success' and data['data']['result']:
            value_bytes = float(data['data']['result'][0]['value'][1])
            return data, value_bytes
        return data, None
    except Exception as e:
        print(f"Error querying {container}: {e}", file=sys.stderr)
        return None, None

def main():
    if len(sys.argv) != 8:
        print("Usage: analyze_memory.py <prom_url> <pod> <duration> <agent_limit_mb> <trace_limit_mb> <sysprobe_limit_mb> <process_limit_mb>", file=sys.stderr)
        sys.exit(1)

    prom_url = sys.argv[1]
    pod = sys.argv[2]
    duration = sys.argv[3]
    agent_limit = int(sys.argv[4])
    trace_limit = int(sys.argv[5])
    sysprobe_limit = int(sys.argv[6])
    process_limit = int(sys.argv[7])
    total_limit = agent_limit + trace_limit + sysprobe_limit + process_limit

    containers = {
        'agent': agent_limit,
        'trace-agent': trace_limit,
        'system-probe': sysprobe_limit,
        'process-agent': process_limit
    }

    results = {}

    for container, limit_mb in containers.items():
        data, value_bytes = query_container(prom_url, pod, container, duration)

        if value_bytes is not None:
            value_mb = value_bytes / 1024 / 1024
            percent = (value_mb / limit_mb) * 100
            results[container] = (value_mb, limit_mb, percent)
            print(f"  {container}: {value_mb:.2f} MB / {limit_mb} MB ({percent:.1f}%)")
        else:
            print(f"  {container}: Could not retrieve metrics")
            results[container] = (0, limit_mb, 0)

    # Calculate total
    total_mb = sum(r[0] for r in results.values())
    total_percent = (total_mb / total_limit) * 100
    print(f"  TOTAL: {total_mb:.2f} MB / {total_limit} MB ({total_percent:.1f}%)")

if __name__ == '__main__':
    main()

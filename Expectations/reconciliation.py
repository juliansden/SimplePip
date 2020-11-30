import os
import sys
import json

def main():
    if len(sys.argv) < 3:
        print("Usage: python reconciliation.py annotation_folder path_instance_folder")
        return 1
    dirs = os.listdir(sys.argv[1])
    if (len(dirs) == 0):
        print("Annotation folder has no file")
        return 1
    if not os.path.exists(sys.argv[2]):
        os.makedirs(sys.argv[2])

    path_instances = {}
    for process in dirs:
        with open(os.path.join(sys.argv[1], process), 'r') as f:
            line = f.readline()
            while line:
                dic = json.loads(line)
                path_id = dic.get('id').get('pid') + '_' + str(dic.get('id').get('seq'))
                dic.pop('id', None)
                if path_id not in path_instances:
                    path_instances[path_id] = {}
                if process not in path_instances[path_id]:
                    path_instances[path_id][process] = []
                path_instances[path_id][process].append(dic)
                line = f.readline()
            
            # {'id': {'pid': 'a', 'seq': 0}, 'name': 'heartbeat', 'type': 'start_task'}
            f.close()

    for path_id, path_instance in path_instances.items():
        for pid, logs in path_instance.items():
            task_stack = []
            new_logs = []
            for log in logs:
                if log['type'] == 'start_task':
                    log["logs"] = []
                    log['type'] = 'task'
                    task_stack.append(log)
                elif log['type'] == 'end_task':
                    if len(task_stack) == 0 or task_stack[-1]["name"] != log["name"]:
                        print(f'Error: cannot match start_task for {pid}::{log["name"]} in process {pid}')
                        return 1
                    new_logs.append(task_stack.pop())
                else:
                    if len(task_stack) == 0:
                        new_logs.append(log)
                    else:
                        task_stack[-1]["logs"].append(log)
            if len(task_stack) != 0:
                print(f'Error: cannot match end_task for {pid}::{task_stack[-1]["name"]} in process {pid}')
            path_instance[pid] = new_logs
        with open(os.path.join(sys.argv[2], path_id), 'w') as f:
            f.write(json.dumps(path_instance, indent=4))

if __name__ == "__main__":
    main()
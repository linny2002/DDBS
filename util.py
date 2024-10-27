import json
import subprocess


def load_jsonl(file_name):
    with open(file_name, "r", encoding="utf-8") as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                yield json.loads(stripped_line)


def dump_jsonl(data, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            f.flush()
            

# get a list of all cotainer names    
def get_container_names(prefix):
    result = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
    if result.returncode != 0:
        print("Error getting container names")
        return []
    return [name for name in result.stdout.splitlines() if name.startswith(prefix)]

#! /bin/env python3
import os, time, signal, argparse

IMAGES = [
    "txcheck-my-sql-container",
    "txcheck-mariadb-container"
]

def get_container_name(nr):
    return f"{IMAGE_NAME}-{nr}"

def container_has_bugs(container_name):
    # Check if there are bugs in the container.
    has_files = os.popen(f"docker exec {container_name} ls").read()
    if not has_files:
        return True
    has_bugs = os.popen(f"docker exec {container_name} ls found_bugs").read()
    return bool(has_bugs)

def start_instances():
    # Start the 10 instances.
    for i in range(NR_INSTANCES):
        command = f"docker run -itd --replace --rm --name {get_container_name(i)} {IMAGE_NAME}"
        os.system(command)

def watch():
    start_time = time.time()
    # Check the status of the containers.
    while True:
        time.sleep(30)
        found_bugs = []
        for instance in range(NR_INSTANCES):
            container_name = get_container_name(instance)
            found_bugs.append(container_has_bugs(container_name))

        duration = time.time() - start_time
        print(f"\n\n\n\n\nDuration / Found bugs:")
        print(f"           {duration // 60 // 60}h {duration // 60 % 60}m {duration % 60:.2f}s")
        for i in range(NR_INSTANCES):
            print(f"{i: 6}   ", end="")
        print("")
        for bug in found_bugs:
            print(str(bug).ljust(6).rjust(9), end="")
        print("")

# Capture ctrl+c to kill all containers.
def signal_handler(sig, frame):
    print("\n\nKilling all containers...")
    os.system("docker kill $(docker ps -q)")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)


parser = argparse.ArgumentParser()
parser.add_argument("command", help="Build or run", type=str, choices=["build", "run"])
parser.add_argument("image", help="Image name", type=str, choices=["mysql", "mariadb"])
parser.add_argument("--instances", help="Number of instances", type=int, default=4, required=False)

args = parser.parse_args()

IMAGE_NAME = IMAGES[0] if args.image == "mysql" else IMAGES[1]
NR_INSTANCES = args.instances

if args.command == "build":
    assert os.path.exists(f"script/{args.image}/Dockerfile"), "Dockerfile not found"
    os.system(f"docker build -t {IMAGE_NAME} -f script/{args.image}/Dockerfile .")
    exit(0)


print("\n\nStarting instances...")
start_instances()

time.sleep(10)
print("\n\nWatching...")
watch()

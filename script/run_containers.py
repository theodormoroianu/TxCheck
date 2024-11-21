#! /bin/env python3
import os, time, signal, argparse

IMAGES = [
    "txcheck-my-sql-container",
    "txcheck-mariadb-container"
]

def run_command(command, show_log=False):
    if show_log:
        print(f"Running {command}")
    os.system(command)

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
    # if the "all_found_bugs" folder exists, move it to "all_found_bugs_old".
    if os.path.exists("all_found_bugs"):
        run_command("mkdir all_found_bugs_old -p", show_log=True)
        for file in os.listdir("all_found_bugs"):
            for bug in os.listdir(f"all_found_bugs/{file}"):
                run_command(f"mv all_found_bugs/{file}/{bug} all_found_bugs_old", show_log=True)
        run_command("rm -r all_found_bugs", show_log=True)

    run_command("mkdir all_found_bugs", show_log=True)

    working_dir = os.getcwd()

    # Start the 10 instances.
    for i in range(NR_INSTANCES):
        folder_name = f"all_found_bugs/{get_container_name(i)}"
        run_command(f"mkdir {folder_name}", show_log=True)
        run_command(
            f"docker run -itd --replace --rm -v {working_dir}/{folder_name}:/home/mysql/found_bugs:z --name {get_container_name(i)} {IMAGE_NAME}", True)
        

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
    command = f"docker build -t {IMAGE_NAME} -f script/{args.image}/Dockerfile ."
    os.system(f"Running {command}")
    exit(0)


print("Starting instances...")
start_instances()

time.sleep(10)
print("\n\nWatching...")
watch()

import os, time

IMAGES = [
    "txcheck-my-sql-container",
    "txcheck-mariadb-container"
]

IMAGE_NAME = IMAGES[1]
NR_INSTANCES = 4

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

print("Killing all containers...")
os.system("docker kill $(docker ps -q)")

print("\n\nStarting instances...")
start_instances()

time.sleep(10)
print("\n\nWatching...")
watch()

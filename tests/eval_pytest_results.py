## !!! THIS IS PART OF THE CI-CD PIPELINE.  DO NOT ALTER OR DELETE !!!
# written: gmcwilliams

import sys
import re
import subprocess

docker_log = subprocess.check_output(["docker", "logs", "-t", "prefect_test_server", "--tail=1"]).decode()

result_failed = re.search(r'(\d+)\sfailed', docker_log)
result_passed = re.search(r'(\d+)\spassed', docker_log)
result_skipped = re.search(r'(\d+)\sskipped', docker_log)
result_warning = re.search(r'(\d+)\swarnings', docker_log)

failed = int(result_failed.group(1)) if result_failed else 0
passed = int(result_passed.group(1)) if result_passed else 0
skipped = int(result_skipped.group(1)) if result_skipped else 0
warning = int(result_warning.group(1)) if result_warning else 0

print("Test Results:")
print(f"Failed: {failed}")
print(f"Passed: {passed}")
print(f"Skipped: {skipped}")
print(f"Warnings: {warning}")

if failed > 0:
    subprocess.run(["docker", "compose", "down"])
    sys.exit(1)
else:
    subprocess.run(["docker", "compose", "down"])
    sys.exit(0)
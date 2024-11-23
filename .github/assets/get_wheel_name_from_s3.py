import sys
from pathlib import Path

import boto3

X86_NAME = "x86"
ARM_NAME = "arm"
X86_FULLNAME = "x86_64"
ARM_FULLNAME = "aarch64"

commit_hash = sys.argv[1]
machine_arch = sys.argv[2]

s3 = boto3.client("s3")
response = s3.list_objects_v2(Bucket="github-actions-artifacts-bucket", Prefix=f"builds/{commit_hash}")
machine_arch_to_wheel_name = {}
for content in response["Contents"]:
    wheel_name = Path(content["Key"]).name
    if X86_FULLNAME in wheel_name:
        machine_arch_to_wheel_name[X86_FULLNAME] = wheel_name
    elif ARM_FULLNAME in wheel_name:
        machine_arch_to_wheel_name[ARM_FULLNAME] = wheel_name

if machine_arch == X86_NAME and X86_FULLNAME in machine_arch_to_wheel_name:
    print(machine_arch_to_wheel_name[X86_FULLNAME])
elif machine_arch == ARM_NAME and ARM_FULLNAME in machine_arch_to_wheel_name:
    print(machine_arch_to_wheel_name[ARM_FULLNAME])
else:
    print("0")

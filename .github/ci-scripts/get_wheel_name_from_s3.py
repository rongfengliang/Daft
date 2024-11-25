import sys
from pathlib import Path

import boto3
import wheellib

if __name__ == "__main__":
    commit_hash = sys.argv[1]
    platform_substring = sys.argv[2]

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket="github-actions-artifacts-bucket", Prefix=f"builds/{commit_hash}/")
    matches = []
    for content in response.get("Contents", []):
        wheelname = Path(content["Key"]).name
        platform_tag = wheellib.get_platform_tag(wheelname)
        if platform_substring in platform_tag:
            matches.append(wheelname)

    if len(matches) > 1:
        raise Exception(
            f"Multiple wheels found that match the given platform substring: {platform_substring}; expected just 1"
        )

    try:
        print(next(iter(matches)))
    except StopIteration:
        pass

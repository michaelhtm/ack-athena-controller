# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the Athena WorkGroup resource"""

import time
import boto3
import pytest

from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest import tags as tagutil
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_athena_resource
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import work_group

WORK_GROUP_RESOURCE_PLURAL = "workgroups"

CREATE_WAIT_SECONDS = 10
MODIFY_WAIT_SECONDS = 10
DELETE_WAIT_SECONDS = 10

@pytest.fixture(scope="module")
def athena_client():
    return boto3.client('athena')

@pytest.fixture(scope="module")
def simple_work_group():
    work_group_name = random_suffix_name("my-simple-work-group", 24)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["WORK_GROUP_NAME"] = work_group_name

    resource_data = load_athena_resource(
        "work_group_simple",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP,
        CRD_VERSION,
        WORK_GROUP_RESOURCE_PLURAL,
        work_group_name,
        namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr)

    try:
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_SECONDS)
        assert deleted
    except:
        pass


@service_marker
@pytest.mark.canary
class TestWorkGroup:
    def test_crud(self, simple_work_group, athena_client):
        ref, _ = simple_work_group

        time.sleep(CREATE_WAIT_SECONDS)
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)

        assert "spec" in cr
        assert "name" in cr["spec"]
        work_group_name = cr["spec"]["name"]

        latest = work_group.get(work_group_name)
        assert latest is not None
        assert "Description" in latest
        description = latest["Description"]
        assert description == "initial description"

        wg_tags = athena_client.list_tags_for_resource(
            ResourceARN=cr["status"]["ackResourceMetadata"]["arn"],
        )["Tags"]
        tags = tagutil.clean(wg_tags)
        assert len(tags) == 2
        assert {"Key": "k1", "Value": "v1"} in tags
        assert {"Key": "k2", "Value": "v2"} in tags

        # update the CR
        updates = {
            "spec": {
                "description": "updated description",
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_SECONDS)

        latest = work_group.get(work_group_name)
        assert latest is not None
        assert "Description" in latest
        description = latest["Description"]
        assert description == "updated description"
        
        # update the tags
        new_tags = [
            {
                "key": "k1",
                "value": "v11"
            },
            {
                "key": "k3",
                "value": "v3"
            }     
        ]
        updates = {
            "spec": {
                "tags": new_tags
            }
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_SECONDS)

        wg_tags = athena_client.list_tags_for_resource(
            ResourceARN=cr["status"]["ackResourceMetadata"]["arn"],
        )["Tags"]
        tags = tagutil.clean(wg_tags)
        assert len(tags) == 2
        assert {"Key": "k1", "Value": "v11"} in tags
        assert {"Key": "k3", "Value": "v3"} in tags
        
        # delete the CR
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_SECONDS)
        assert deleted
        work_group.wait_until_deleted(work_group_name)
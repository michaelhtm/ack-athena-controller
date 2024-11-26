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

"""Integration tests for the Athena PreparedStatement resource"""

import time
import pytest

from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_athena_resource
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import prepared_statement

from .test_work_group import simple_work_group

PREPARED_STATEMENT_RESOURCE_PLURAL = "preparedstatements"

CREATE_WAIT_SECONDS = 20
MODIFY_WAIT_SECONDS = 10
DELETE_WAIT_SECONDS = 10


@pytest.fixture(scope="module")
def simple_prepared_statement(simple_work_group):
    (ref, cr) = simple_work_group
    global work_group_name
    work_group_name = cr["spec"]["name"]
    
    # prepared_statement_name = random_suffix_name("mysimpleps", 24)
    prepared_statement_name = "myps1111"
    
    replacements = REPLACEMENT_VALUES.copy()
    replacements["PREPARED_STATEMENT_NAME"] = prepared_statement_name
    replacements["WORK_GROUP_NAME"] = work_group_name

    resource_data = load_athena_resource(
        "prepared_statement_simple",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP,
        CRD_VERSION,
        PREPARED_STATEMENT_RESOURCE_PLURAL,
        prepared_statement_name,
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
class TestPreparedStatement:
    def test_crud(self, simple_prepared_statement):
        ref, _ = simple_prepared_statement

        time.sleep(CREATE_WAIT_SECONDS)
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)

        assert "spec" in cr
        assert "name" in cr["spec"]
        prepared_statement_name = cr["spec"]["name"]

        latest = prepared_statement.get(prepared_statement_name, work_group_name)
        assert latest is not None
        assert "Description" in latest
        assert "QueryStatement" in latest
        description = latest["Description"]
        queryStatement = latest["QueryStatement"]
        assert description == "initial description"
        assert queryStatement == "SELECT * FROM my_table WHERE column = ?"

        # update the CR
        updates = {
            "spec": {
                "description": "updated description",
                "queryStatement": "SELECT * FROM my_updated_table WHERE column = ?",
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_SECONDS)

        latest = prepared_statement.get(prepared_statement_name, work_group_name)
        assert latest is not None
        assert "Description" in latest
        assert "QueryStatement" in latest
        description = latest["Description"]
        queryStatement = latest["QueryStatement"]
        assert description == "updated description"
        assert queryStatement == "SELECT * FROM my_updated_table WHERE column = ?"

        # delete the CR
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_SECONDS)
        assert deleted
        prepared_statement.wait_until_deleted(prepared_statement_name, work_group_name)

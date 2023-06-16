#!/usr/bin/env python3

# Author: Eric Turgeon
# License: BSD
# Location for tests into REST API of FreeNAS

import pytest
import sys
import json
import os
import time
from pytest_dependency import depends
apifolder = os.getcwd()
sys.path.append(apifolder)
from functions import POST, GET, DELETE, PUT, SSH_TEST, wait_on_job
from auto_config import pool_name, ha, password, user, ip

# shell = '/bin/csh'
group_id = GET('/group/?group=wheel', controller_a=ha).json()[0]['id']
dataset = f"{pool_name}/test_homes"
dataset_url = dataset.replace('/', '%2F')

home_files = {
    "~/": "0o40750",
    "~/.profile": "0o100644",
    "~/.ssh": "0o40700",
    "~/.ssh/authorized_keys": "0o100600",
}

home_acl = [
    {
        "tag": "owner@",
        "id": None,
        "type": "ALLOW",
        "perms": {"BASIC": "FULL_CONTROL"},
        "flags": {"BASIC": "INHERIT"}
    },
    {
        "tag": "group@",
        "id": None,
        "type": "ALLOW",
        "perms": {"BASIC": "FULL_CONTROL"},
        "flags": {"BASIC": "INHERIT"}
    },
    {
        "tag": "everyone@",
        "id": None,
        "type": "ALLOW",
        "perms": {"BASIC": "TRAVERSE"},
        "flags": {"BASIC": "NOINHERIT"}
    },
]


def check_config_file(file_name, expected_line):
    results = SSH_TEST(f'cat {file_name}', user, password, ip)
    assert results['result'], f'out: {results["output"]}, err: {results["stderr"]}'
    assert expected_line in results['output'].splitlines(), f'out: {results["output"]}, err: {results["stderr"]}'


@pytest.mark.dependency(name="user_01")
def test_01_get_next_uid(request):
    results = GET('/user/get_next_uid/')
    assert results.status_code == 200, results.text
    global next_uid
    next_uid = results.json()


@pytest.mark.dependency(name="user_02")
def test_02_creating_user_testuser(request):
    depends(request, ["user_01"])
    """
    Test for basic user creation. In this case 'smb' is disabled to bypass
    passdb-related code. This is because the passdb add relies on users existing
    in passwd database, and errors during error creation will get masked as
    passdb errors.
    """
    global user_id
    payload = {
        "username": "testuser",
        "full_name": "Test User",
        "group_create": True,
        "password": "test1234",
        "uid": next_uid,
        "smb": False,
        "shell": '/bin/csh'
    }
    results = POST("/user/", payload)
    assert results.status_code == 200, results.text
    user_id = results.json()


def test_03_verify_post_user_do_not_leak_password_in_middleware_log(request):
    depends(request, ["user_01", "ssh_password"], scope="session")
    cmd = """grep -R "test1234" /var/log/middlewared.log"""
    results = SSH_TEST(cmd, user, password, ip)
    assert results['result'] is False, str(results['output'])


def test_04_look_user_is_created(request):
    depends(request, ["user_02", "user_01"])
    results = GET('/user?username=testuser')
    assert len(results.json()) == 1, results.text

    u = results.json()[0]
    g = u['group']
    to_check = [
        {
            'file': '/etc/master.passwd',
            'value': f"testuser:{u['unixhash']}:{u['uid']}:{u['group']['bsdgrp_gid']}::0:0:{u['full_name']}:{u['home']}:{u['shell']}"
        },
        {
            'file': '/etc/group',
            'value': f'{g["bsdgrp_group"]}:*:{g["bsdgrp_gid"]}:'
        }
    ]
    for f in to_check:
        check_config_file(f['file'], f['value'])


def test_05_check_user_exists(request):
    """
    get_user_obj is a wrapper around the pwd module.
    This check verifies that the user is _actually_ created.
    """
    payload = {
        "username": "testuser"
    }
    results = POST("/user/get_user_obj/", payload)
    assert results.status_code == 200, results.text
    if results.status_code == 200:
        pw = results.json()
        assert pw['pw_uid'] == next_uid, results.text
        assert pw['pw_shell'] == '/bin/csh', results.text
        assert pw['pw_gecos'] == 'Test User', results.txt
        assert pw['pw_dir'] == '/nonexistent', results.txt


def test_06_get_user_info(request):
    depends(request, ["user_02", "user_01"])
    global userinfo
    userinfo = GET(f'/user/id/{user_id}').json()


def test_07_look_user_name(request):
    depends(request, ["user_02", "user_01"])
    assert userinfo["username"] == "testuser"


def test_08_look_user_full_name(request):
    depends(request, ["user_02", "user_01"])
    assert userinfo["full_name"] == "Test User"


def test_09_look_user_uid(request):
    depends(request, ["user_02", "user_01"])
    assert userinfo["uid"] == next_uid


def test_10_look_user_shell(request):
    depends(request, ["user_02", "user_01"])
    assert userinfo["shell"] == '/bin/csh'


def test_11_add_employee_id_and_team_special_attributes(request):
    depends(request, ["user_02", "user_01"])
    payload = {
        'key': 'Employee ID',
        'value': 'TU1234',
        'key': 'Team',
        'value': 'QA'
    }
    results = POST(f"/user/id/{user_id}/set_attribute/", payload)
    assert results.status_code == 200, results.text


def test_12_get_new_next_uid(request):
    depends(request, ["user_02", "user_01"])
    results = GET('/user/get_next_uid/')
    assert results.status_code == 200, results.text
    global new_next_uid
    new_next_uid = results.json()


def test_13_next_and_new_next_uid_not_equal(request):
    depends(request, ["user_02", "user_01"])
    assert new_next_uid != next_uid


def test_14_setting_user_groups(request):
    depends(request, ["user_02", "user_01"])
    payload = {'groups': [group_id]}
    GET('/user?username=testuser').json()[0]['id']
    results = PUT(f"/user/id/{user_id}/", payload)
    assert results.status_code == 200, results.text


# Update tests
# Update the testuser
def test_15_updating_user_testuser_info(request):
    depends(request, ["user_02", "user_01"])
    payload = {"full_name": "Test Renamed",
               "password": "testing123",
               "uid": new_next_uid}
    results = PUT(f"/user/id/{user_id}/", payload)
    assert results.status_code == 200, results.text


def test_16_verify_put_user_do_not_leak_password_in_middleware_log(request):
    depends(request, ["user_02", "user_01", "ssh_password"], scope="session")
    cmd = """grep -R "testing123" /var/log/middlewared.log"""
    results = SSH_TEST(cmd, user, password, ip)
    assert results['result'] is False, str(results['output'])


def test_17_get_user_new_info(request):
    depends(request, ["user_02", "user_01"])
    global userinfo
    userinfo = GET('/user?username=testuser').json()[0]


def test_18_look_user_full_name(request):
    depends(request, ["user_02", "user_01"])
    assert userinfo["full_name"] == "Test Renamed"


def test_19_look_user_new_uid(request):
    depends(request, ["user_02", "user_01"])
    assert userinfo["uid"] == new_next_uid


def test_20_look_user_groups(request):
    depends(request, ["user_02", "user_01"])
    assert userinfo["groups"] == [group_id]


def test_21_remove_old_team_special_atribute(request):
    depends(request, ["user_02", "user_01"])
    payload = 'Team'
    results = POST(f"/user/id/{user_id}/pop_attribute/", payload)
    assert results.status_code == 200, results.text


def test_22_add_new_team_to_special_atribute(request):
    depends(request, ["user_02", "user_01"])
    payload = {'key': 'Team', 'value': 'QA'}
    results = POST(f"/user/id/{user_id}/set_attribute/", payload)
    assert results.status_code == 200, results.text


# Delete the testuser
def test_23_deleting_user_testuser(request):
    depends(request, ["user_02", "user_01"])
    results = DELETE(f"/user/id/{user_id}/", {"delete_group": True})
    assert results.status_code == 200, results.text


def test_24_look_user_is_delete(request):
    depends(request, ["user_02", "user_01"])
    assert len(GET('/user?username=testuser').json()) == 0


def test_25_has_root_password(request):
    depends(request, ["user_02", "user_01"])
    assert GET('/user/has_root_password/', anonymous=True).json() is True


def test_26_get_next_uid_for_shareuser(request):
    depends(request, ["user_02", "user_01"])
    results = GET('/user/get_next_uid/')
    assert results.status_code == 200, results.text
    global next_uid
    next_uid = results.json()


@pytest.mark.dependency(name="shareuser")
def test_27_creating_shareuser_to_test_sharing(request):
    depends(request, ["user_02", "user_01"])
    payload = {
        "username": "shareuser",
        "full_name": "Share User",
        "group_create": True,
        "groups": [group_id],
        "password": "testing",
        "uid": next_uid,
        "shell": '/bin/csh'
    }
    results = POST("/user/", payload)
    assert results.status_code == 200, results.text


def test_28_verify_post_user_do_not_leak_password_in_middleware_log(request):
    depends(request, ["ssh_password"], scope="session")
    cmd = """grep -R "testing" /var/log/middlewared.log"""
    results = SSH_TEST(cmd, user, password, ip)
    assert results['result'] is False, str(results['output'])


def test_29_get_next_uid_for_homes_check(request):
    results = GET('/user/get_next_uid/')
    assert results.status_code == 200, results.text
    global next_uid
    next_uid = results.json()


@pytest.mark.dependency(name="HOME_DS_CREATED")
def test_30_creating_home_dataset(request):
    """
    SMB share_type is selected for this test so that
    we verify that ACL is being stripped properly from
    the newly-created home directory.
    """
    depends(request, ["pool_04"], scope="session")
    payload = {
        "name": dataset,
        "share_type": "SMB"
    }
    results = POST("/pool/dataset/", payload)
    assert results.status_code == 200, results.text

    results = POST(
        f'/pool/dataset/id/{dataset_url}/permission/', {
            'acl': home_acl,
        }
    )
    assert results.status_code == 200, results.text
    perm_job = results.json()
    job_status = wait_on_job(perm_job, 180)
    assert job_status['state'] == 'SUCCESS', str(job_status['results'])


@pytest.mark.dependency(name="USER_CREATED")
def test_31_creating_user_with_homedir(request):
    depends(request, ["HOME_DS_CREATED"])
    global user_id
    payload1 = {
        "username": "testuser2",
        "full_name": "Test User2",
        "group_create": True,
        "password": "test1234",
        "uid": next_uid,
        "shell": '/bin/csh',
        "sshpubkey": "canary",
        "home": f'/mnt/{dataset}/testuser2',
        "home_mode": '750'
    }
    results = POST("/user/", payload1)
    assert results.status_code == 200, results.text
    user_id = results.json()
    time.sleep(5)

    payload2 = {"username": "testuser2"}
    results = POST("/user/get_user_obj/", payload2)
    assert results.status_code == 200, results.text

    pw = results.json()
    assert pw['pw_dir'] == payload1['home'], results.text
    assert pw['pw_name'] == payload1['username'], results.text
    assert pw['pw_uid'] == payload1['uid'], results.text
    assert pw['pw_shell'] == payload1['shell'], results.text
    assert pw['pw_gecos'] == payload1['full_name'], results.text


def test_32_verify_post_user_do_not_leak_password_in_middleware_log(request):
    depends(request, ["USER_CREATED", "ssh_password"], scope="session")
    cmd = """grep -R "test1234" /var/log/middlewared.log"""
    results = SSH_TEST(cmd, user, password, ip)
    assert results['result'] is False, str(results['output'])


def test_33_smb_user_passb_entry_exists(request):
    depends(request, ["USER_CREATED", "ssh_password"], scope="session")
    cmd = "midclt call smb.passdb_list true"
    results = SSH_TEST(cmd, user, password, ip)
    assert results['result'] is True, f'out: {results["output"]}, err: {results["stderr"]}'
    pdb_list = json.loads(results['output'])
    my_entry = None
    for entry in pdb_list:
        if entry['Unix username'] == "testuser2":
            my_entry = entry
            break

    assert my_entry is not None, f'out: {results["output"]}, err: {results["stderr"]}'
    assert my_entry["Account Flags"] == "[U          ]", str(my_entry)

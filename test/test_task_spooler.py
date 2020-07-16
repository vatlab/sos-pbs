#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import pytest

import subprocess
import shutil
from sos import execute_workflow

has_docker = True
try:
    subprocess.check_output('docker ps | grep test_sos', shell=True).decode()
except subprocess.CalledProcessError:
    subprocess.call('sh build_test_docker.sh', shell=True)
    try:
        subprocess.check_output(
            'docker ps | grep test_sos', shell=True).decode()
    except subprocess.CalledProcessError:
        print('Failed to set up a docker machine with sos')
        has_docker = False


@pytest.mark.skipif(not shutil.which('ts'), reason="ts command not found")
def test_local_ts(clear_now_and_after):
    #
    clear_now_and_after('a.txt')

    execute_workflow(
        '''
        [10]
        task:
        sh:
        echo "I am done" >> a.txt
        ''',
        options={
            'config_file': '~/docker.yml',
            'wait_for_task': True,
            'default_queue': 'local_ts',
            'sig_mode': 'force',
        })
    assert os.path.isfile('a.txt')


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_remote_ts(clear_now_and_after, temp_factory):
    clear_now_and_after('ar.txt')

    temp_factory(
        'remote_ts.sos',
        content='''
[10]
task:
sh:
echo "I am done" >> ar.txt
''')
    ret = subprocess.call('sos run remote_ts -c ~/docker.yml -q ts', shell=True)
    assert ret == 0
    ret = subprocess.call(
        'sos remote pull ts --files ar.txt -c ~/docker.yml', shell=True)
    assert ret == 0
    assert os.path.isfile('ar.txt')

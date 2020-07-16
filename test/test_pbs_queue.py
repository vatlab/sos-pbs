#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import sys
import pytest
import random

import subprocess

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


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_remote_execute(clear_now_and_after):
    clear_now_and_after('result.txt')
    execute_workflow(
        '''
        [10]
        output: 'result.txt'
        task:

        run:
        echo 'a' > 'result.txt'

        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })
    assert os.path.isfile('result.txt')
    with open('result.txt') as res:
        assert res.read() == 'a\n'


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_task_spooler_with_force_sigmode(purge_tasks):
    execute_workflow(
        '''
        [10]
        input: for_each={'i': range(3)}
        task:

        run: expand=True
            echo I am spooler with force {i}
            sleep {10 + i*2}
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })


# @pytest.mark.skipif(
#     sys.platform == 'win32' or not has_docker,
#     reason='No symbloc link problem under win32 or no docker')
# def test_to_host_rename(clear_now_and_after):
#     '''Test to_host with dictionary'''
#     clear_now_and_after('1.txt', '2.txt', '3.txt')
#     execute_workflow(
#         r'''
#         [1]
#         sh:
#         echo "1" > 1.txt

#         [10]
#         task: to_host={'1.txt': '2.txt'}, from_host={'3.txt': '2.txt'}
#         with open('2.txt', 'a') as t:
#             t.write('2\n')
#         ''',
#         options={
#             'config_file': '~/docker.yml',
#             'default_queue': 'ts',
#             'sig_mode': 'force',
#         })

#     assert os.path.isfile('3.txt')
#     with open('3.txt') as txt:
#         content = txt.read()
#         assert '1\n2\n' == content


@pytest.mark.skipif(
    sys.platform == 'win32' or not has_docker,
    reason='No symbloc link problem under win32 or no docker')
def test_send_symbolic_link(clear_now_and_after, temp_factory):
    '''Test to_host symbolic link or directories that contain symbolic link. #508'''
    # create a symbloc link
    temp_factory('ttt.py', content='something')
    clear_now_and_after('llink')

    subprocess.call('ln -s ttt.py llink', shell=True)
    execute_workflow(
        '''
        import os
        [10]
        task: to_host='llink'
        sz = os.path.getmtime('llink')
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force'
        })


@pytest.mark.skipif(
    not os.path.exists(os.path.expanduser('~').upper()) or not has_docker,
    reason='Skip test for case sensitive file system')
def test_case_insensitive_local_path(clear_now_and_after, temp_factory):
    '''Test path_map from a case insensitive file system.'''
    clear_now_and_after('test_pbs_queue.py.bak', 'tt1.py.bak')

    temp_factory('tt1.py', content='something')
    execute_workflow(
        '''
[10]
output: 'tt1.py.bak'
task: to_host=r'{}'
import shutil
shutil.copy("tt1.py", f"{{_output}}")
'''.format(os.path.join(os.path.abspath('.').upper(), 'tt1.py')),
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })

    assert os.path.isfile('tt1.py.bak')
    # the files should be the same
    with open('tt1.py') as ori, open('tt1.py.bak') as bak:
        assert ori.read() == bak.read()


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_sos_execute():
    '''Test sos execute'''
    subprocess.check_output(
        'sos purge -c ~/docker.yml -q docker --all', shell=True)
    execute_workflow(
        '''
        [10]
        input: for_each={'i': range(3)}
        task:

        run: expand=True
            echo Testing purge {i}
            sleep {i*2}
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_sos_purge():
    '''Test purge tasks'''
    # purge all previous tasks
    subprocess.check_output('sos purge --all -c ~/docker.yml -q ts', shell=True)
    execute_workflow(
        '''
        [10]
        input: for_each={'i': range(3)}
        task:

        run: expand=True
            echo Testing purge {i}
            sleep {i*2}
    ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_remote_input(clear_now_and_after):
    '''Test remote target'''
    clear_now_and_after('test_file.txt')
    # purge all previous tasks
    execute_workflow(
        '''
        [10]
        task:
        run:
            echo A file >> "test_file.txt"
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })
    # this file is remote only
    assert not os.path.isfile('test_file.txt')


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_remote_input_1(clear_now_and_after):
    #
    clear_now_and_after('test1.txt')
    execute_workflow(
        '''
        [10]
        input: remote('test_file.txt')
        output: 'test1.txt'
        task:
        run: expand=True
            echo {_input} >> {_output}
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })
    #
    assert not os.path.isfile('test_file.txt')
    assert os.path.isfile('test1.txt')


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_no_remote_input(clear_now_and_after):
    '''Test remote target'''
    clear_now_and_after('test_file_A.txt', 'test_file_B.txt')
    execute_workflow(
        '''
    [10]
    task:
    run:
        echo A file >> "test_file_A.txt"
        echo B file >> "test_file_B.txt"
    ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })
    # this file is remote only
    assert not os.path.isfile('test_file_A.txt')
    assert not os.path.isfile('test_file_B.txt')


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_multiple_remote_input(clear_now_and_after):
    #
    clear_now_and_after('test1.txt')
    execute_workflow(
        '''
        [10]
        A = 'test_file_A.txt'
        input: remote(A, ['test_file_B.txt'])
        output: 'test1.txt'
        task:
        run: expand=True
            cat {_input} >> {_output}
    ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })
    #
    assert not os.path.isfile('test_file_A.txt')
    assert not os.path.isfile('test_file_B.txt')
    assert os.path.isfile('test1.txt')
    with open('test1.txt') as w:
        content = w.read()
        assert 'A file' in content
        assert 'B file' in content


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_remote_output(clear_now_and_after):
    '''Test remote target'''
    # purge all previous tasks
    clear_now_and_after('test_file.txt', 'test_file1.txt')

    execute_workflow(
        '''
        [10]
        output: remote('test_file.txt'), 'test_file1.txt'
        task:
        run:
            echo A file >> "test_file.txt"
            echo A file >> "test_file1.txt"
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })
    # this file is remote only
    assert not os.path.isfile('test_file.txt')
    assert os.path.isfile('test_file1.txt')


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_from_host_option(clear_now_and_after):
    '''Test from_remote option'''
    clear_now_and_after('llp')

    execute_workflow(
        '''
        [10]
        task: from_host='llp'
        with open('llp', 'w') as llp:
            llp.write("LLP")
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })

    assert os.path.isfile('llp')


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_local_from_host_option(clear_now_and_after):
    '''Test from_remote option'''
    clear_now_and_after('llp')

    execute_workflow(
        '''
        [10]
        task: from_host='llp'
        with open('llp', 'w') as llp:
            llp.write("LLP")
        ''',
        options={
            'config_file': '~/docker.yml',
            'sig_mode': 'force',
            'default_queue': 'localhost',
        })
    assert os.path.isfile('llp')


def test_list_hosts():
    '''test list hosts using sos status -q'''
    for v in ['0', '1', '3', '4']:
        # ts of type pbs should be in output
        output = subprocess.check_output(
            ['sos', 'remote', 'list', '-c', '~/docker.yml', '-v', v]).decode()
        assert 'ts' in output


def test_sync_input_output(clear_now_and_after):
    '''Test sync input and output with remote host'''
    clear_now_and_after([f'test_{i}.txt' for i in range(4)],
                        [f'test_{i}.bak' for i in range(4)])
    val = random.randint(1, 10000)

    execute_workflow(
        '''
        parameter: g = 100

        [10]
        input: for_each=dict(i=range(4))
        output: f'test_{i}.txt'

        with open(f'test_{i}.txt', 'w') as tst:
            tst.write(f'test_{i}_{g}')

        [20]
        output: _input.with_suffix('.bak')

        task:

        with open(_input, 'r') as inf, open(_output, 'w') as outf:
            outf.write(inf.read() + '.bak')
    ''',
        args=['--g', str(val)],
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })
    # now check if
    for i in range(4):
        assert os.path.isfile(f'test_{i}.txt')
        with open(f'test_{i}.bak') as outf:
            assert outf.read() == f'test_{i}_{val}.bak'
        assert os.path.isfile(f'test_{i}.bak')
        with open(f'test_{i}.bak') as outf:
            assert outf.read() == f'test_{i}_{val}.bak'


def test_sync_master_task(clear_now_and_after):
    '''Test sync input and output with remote host with trunksize'''
    clear_now_and_after([f'test_{i}.txt' for i in range(4)],
                        [f'test_{i}.bak' for i in range(4)])
    val = random.randint(1, 10000)
    execute_workflow(
        '''
        parameter: g = 100

        [10]
        input: for_each=dict(i=range(4))
        output: f'test_{i}.txt'

        with open(f'test_{i}.txt', 'w') as tst:
            tst.write(f'test_{i}_{g}')

        [20]
        output: _input.with_suffix('.bak')

        task: trunk_size=2

        with open(_input, 'r') as inf, open(_output, 'w') as outf:
            outf.write(inf.read() + '.bak')
        ''',
        args=['--g', str(val)],
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })

    # now check if
    for i in range(4):
        assert os.path.isfile(f'test_{i}.txt')
        with open(f'test_{i}.bak') as outf:
            assert outf.read() == f'test_{i}_{val}.bak'
        assert os.path.isfile(f'test_{i}.bak')
        with open(f'test_{i}.bak') as outf:
            assert outf.read() == f'test_{i}_{val}.bak'


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_delayed_interpolation(clear_now_and_after):
    '''Test delayed interpolation with expression involving remote objects'''
    # purge all previous tasks
    clear_now_and_after('test.py', 'test.py.bak')
    execute_workflow(
        '''
        [10]
        output: remote('test.py')
        task:
        run:
        touch test.py

        [20]
        output: remote(f"{_input:R}.bak")
        task:
        run: expand=True
        cp {_input} {_output}
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })
    # this file is remote only
    assert not os.path.isfile('test.py')
    assert not os.path.isfile('test.py.bak')


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_remote_execution(purge_tasks):
    execute_workflow(
        '''
        [10]
        input: for_each={'i': range(5)}
        task:

        run: expand=True
        echo I am {i}
        sleep {5+i}
        ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'max_running_jobs': 5,
            'sig_mode': 'force',
        })


@pytest.mark.skipif(not has_docker, reason="Docker container not usable")
def test_task_spooler(purge_tasks):
    '''Test task spooler PBS engine'''
    execute_workflow(
        '''
        [10]
        input: for_each={'i': range(3)}
        task:

        run: expand=True
        echo I am task spooler {i}
        sleep {5+i*2}
    ''',
        options={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        })

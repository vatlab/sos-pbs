#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import sys
import unittest

from sos.parser import SoS_Script
from sos.utils import env
from sos.workflow_executor import Base_Executor
from sos.targets import file_target
from sos.hosts import Host
import subprocess

has_docker = True
try:
    subprocess.check_output('docker ps | grep test_sos', shell=True).decode()
except subprocess.CalledProcessError:
    subprocess.call('sh build_test_docker.sh', shell=True)
    try:
        subprocess.check_output('docker ps | grep test_sos', shell=True).decode()
    except subprocess.CalledProcessError:
        print('Failed to set up a docker machine with sos')
        has_docker = False

#if sys.platform == 'win32':
#    with open('~/docker.yml', 'r') as d:
#        cfg = d.read()
#    with open('~/docker.yml', 'w') as d:
#        d.write(cfg.replace('/home/', 'c:\\Users\\'))

class TestPBSQueue(unittest.TestCase):
    def setUp(self):
        env.reset()
        #self.resetDir('~/.sos')
        self.temp_files = []
        Host.reset()
        # remove .status file left by failed workflows.
        subprocess.call('sos purge', shell=True)

    def tearDown(self):
        for f in self.temp_files:
            if file_target(f).exists():
                file_target(f).unlink()


    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testRemoteExecute(self):
        script = SoS_Script('''
[10]
output: 'result.txt'
task:

run:
  echo 'a' > 'result.txt'

''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        self.assertTrue(file_target('result.txt').target_exists())
        with open('result.txt') as res:
            self.assertEqual(res.read(), 'a\n')

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testRemoteExecution(self):
        subprocess.check_output('sos purge', shell=True).decode()
        script = SoS_Script('''
[10]
input: for_each={'i': range(5)}
task:

run: expand=True
    echo I am {i}
    sleep {5+i}
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'max_running_jobs': 5,
                'sig_mode': 'force',
                }).run()

        Host.reset()
        # until we run the workflow again
        #st = time.time()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'resume_mode': True,
                }).run()


    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testTaskSpooler(self):
        '''Test task spooler PBS engine'''
        subprocess.check_output('sos purge', shell=True).decode()
        script = SoS_Script('''
[10]
input: for_each={'i': range(3)}
task:

run: expand=True
    echo I am task spooler {i}
    sleep {5+i*2}
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        # until we run the workflow again
        #st = time.time()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'resume_mode': True,
                }).run()

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testTaskSpoolerWithForceSigMode(self):
        subprocess.check_output('sos purge', shell=True).decode()
        script = SoS_Script('''
[10]
input: for_each={'i': range(3)}
task:

run: expand=True
    echo I am spooler with force {i}
    sleep {10 + i*2}
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        # until we run the workflow again
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                #
                # This is the only difference, because running with -s force would still
                # skip some of the completed task.
                'sig_mode': 'force',
                'resume_mode': True,
                }).run()
        # out = subprocess.check_output('sos status {} -c ~/docker.yml'.format(tasks), shell=True).decode()
        # self.assertEqual(out.count('completed'), len(res['pending_tasks']))

    @unittest.skipIf(sys.platform == 'win32' or not has_docker, 'No symbloc link problem under win32 or no docker')
    def testToHostRename(self):
        '''Test to_host with dictionary'''
        script = SoS_Script('''
[1]
sh:
   echo "1" > 1.txt

[10]
task: to_host={'1.txt': '2.txt'}, from_host={'3.txt': '2.txt'}
sh:
  echo "2" >> 2.txt
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        self.assertTrue(os.path.isfile('3.txt'))
        with open('3.txt') as txt:
            content = txt.read()
            self.assertEqual('1\n2\n', content, 'Got {}'.format(content))

    @unittest.skipIf(sys.platform == 'win32' or not has_docker, 'No symbloc link problem under win32 or no docker')
    def testSendSymbolicLink(self):
        '''Test to_host symbolic link or directories that contain symbolic link. #508'''
        # create a symbloc link
        with open('ttt.py', 'w') as ttt:
            ttt.write('something')
        if os.path.exists('llink'):
            os.remove('llink')
        subprocess.call('ln -s ttt.py llink', shell=True)
        script = SoS_Script('''
import os
[10]
task: to_host='llink'
sz = os.path.getmtime('llink')
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force'
                }).run()
        os.remove('llink')

    @unittest.skipIf(not os.path.exists(os.path.expanduser('~').upper()) or not has_docker, 'Skip test for case sensitive file system')
    def testCaseInsensitiveLocalPath(self):
        '''Test path_map from a case insensitive file system.'''
        if file_target('test_pbs_queue.py.bak').exists():
            file_target('test_pbs_queue.py.bak').unlink()
        with open('tt1.py', 'w') as tt1:
            tt1.write('soemthing')
        script = SoS_Script('''
[10]
output: 'tt1.py.bak'
task: to_host=r'{}'
import shutil
shutil.copy("tt1.py", f"{{_output}}")
'''.format(os.path.join(os.path.abspath('.').upper(), 'tt1.py')))
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        self.assertTrue(file_target('tt1.py.bak').target_exists('target'))
        # the files should be the same
        with open('tt1.py') as ori, open('tt1.py.bak') as bak:
            self.assertEqual(ori.read(), bak.read())


    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testSoSExecute(self):
        '''Test sos execute'''
        subprocess.check_output('sos purge -c ~/docker.yml -q docker', shell=True)
        script = SoS_Script('''
[10]
input: for_each={'i': range(3)}
task:

run: expand=True
    echo Testing purge {i}
    sleep {i*2}
''')
        wf = script.workflow()
        res = Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testSoSPurge(self):
        '''Test purge tasks'''
        # purge all previous tasks
        subprocess.check_output('sos purge --all -c ~/docker.yml -q ts', shell=True)
        script = SoS_Script('''
[10]
input: for_each={'i': range(3)}
task:

run: expand=True
    echo Testing purge {i}
    sleep {i*2}
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()


    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testRemoteInput(self):
        '''Test remote target'''
        # purge all previous tasks
        script = SoS_Script('''
[10]
task:
run:
    echo A file >> "test_file.txt"
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        # this file is remote only
        self.assertFalse(os.path.isfile('test_file.txt'))
        #
        if file_target('test1.txt').exists():
            file_target('test1.txt').unlink()
        script = SoS_Script('''
[10]
input: remote('test_file.txt')
output: 'test1.txt'
task:
run: expand=True
    echo {_input} >> {_output}
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        #
        self.assertFalse(os.path.isfile('test_file.txt'))
        self.assertTrue(os.path.isfile('test1.txt'))

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testMultipleRemoteInput(self):
        '''Test remote target'''
        script = SoS_Script('''
[10]
task:
run:
    echo A file >> "test_file_A.txt"
    echo B file >> "test_file_B.txt"
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        # this file is remote only
        self.assertFalse(os.path.isfile('test_file_A.txt'))
        self.assertFalse(os.path.isfile('test_file_B.txt'))
        #
        if file_target('test1.txt').exists():
            file_target('test1.txt').unlink()
        script = SoS_Script('''
[10]
A = 'test_file_A.txt'
input: remote(A, ['test_file_B.txt'])
output: 'test1.txt'
task:
run: expand=True
    cat {_input} >> {_output}
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        #
        self.assertFalse(os.path.isfile('test_file_A.txt'))
        self.assertFalse(os.path.isfile('test_file_B.txt'))
        self.assertTrue(os.path.isfile('test1.txt'))
        with open('test1.txt') as w:
            content = w.read()
            self.assertTrue('A file' in content, 'Got {}'.format(content))
            self.assertTrue('B file' in content, 'Got {}'.format(content))

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testRemoteOutput(self):
        '''Test remote target'''
        # purge all previous tasks
        if file_target('test_file.txt').exists():
            file_target('test_file.txt').unlink()
        if file_target('test_file1.txt').exists():
            file_target('test_file1.txt').unlink()
        script = SoS_Script('''
[10]
output: remote('test_file.txt'), 'test_file1.txt'
task:
run:
    echo A file >> "test_file.txt"
    echo A file >> "test_file1.txt"
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        # this file is remote only
        self.assertFalse(os.path.isfile('test_file.txt'))
        self.assertTrue(os.path.isfile('test_file1.txt'))

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testDelayedInterpolation(self):
        '''Test delayed interpolation with expression involving remote objects'''
        # purge all previous tasks
        if file_target('test.py').exists():
            file_target('test.py').unlink()
        if file_target('test.py.bak').exists():
            file_target('test.py.bak').unlink()
        script = SoS_Script('''
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
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        # this file is remote only
        self.assertFalse(os.path.isfile('test.py'))
        self.assertFalse(os.path.isfile('test.py.bak'))


    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testFromHostOption(self):
        '''Test from_remote option'''
        if os.path.isfile('llp'):
            os.remove('llp')
        script = SoS_Script('''
[10]
task: from_host='llp'
with open('llp', 'w') as llp:
    llp.write("LLP")
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                'sig_mode': 'force',
                }).run()
        self.assertTrue(os.path.isfile('llp'))
        os.remove('llp')
        # dict form
        script = SoS_Script('''
[10]
task: from_host={'llp': 'll'}
with open('llp', 'w') as llp:
    llp.write("LLP")
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'default_queue': 'ts',
                }).run()
        self.assertTrue(os.path.isfile('llp'))
        os.remove('llp')

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testLocalFromHostOption(self):
        '''Test from_remote option'''
        if os.path.isfile('llp'):
            os.remove('llp')
        script = SoS_Script('''
[10]
task: from_host='llp'
with open('llp', 'w') as llp:
    llp.write("LLP")
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'sig_mode': 'force',
                'default_queue': 'localhost',
                }).run()
        self.assertTrue(os.path.isfile('llp'))
        os.remove('llp')
        # dict form
        script = SoS_Script('''
[10]
task: from_host={'llp': 'll'}
with open('ll', 'w') as llp:
    llp.write("LLP")
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'sig_mode': 'force',
                'default_queue': 'localhost',
                }).run()
        self.assertTrue(os.path.isfile('llp'))
        os.remove('llp')

    @unittest.skipIf(sys.platform == 'win32' or not has_docker, 'appveyor does not have docker with linux')
    def testRemoteTaskFromJupyter(self):
        '''Test the execution of tasks with -q '''
        from ipykernel.tests.utils import execute
        from sos_notebook.test_utils import sos_kernel, get_display_data
        subprocess.call(['sos', 'purge'])
        with sos_kernel() as kc:
            # the cell will actually be executed several times
            # with automatic-reexecution
            code = """
%run -s force -q ts -c ~/docker.yml
[10]
input: for_each={'i': range(2)}
task:
run: expand=True
   echo this is jupyter pending test "{i}"
   sleep  {10+i}

"""
            # these should be automatically rerun by the frontend
            execute(kc=kc, code=code)
            res = get_display_data(kc.iopub_channel, 'text/html')
            # check for task?
            execute(kc=kc, code='%tasks -q ts')
            res = get_display_data(kc.iopub_channel, 'text/html')
            self.assertTrue('table_ts_' in res, 'Got {}'.format(res))
            # get IDs
            # table_localhost_ac755352394584f797cebddf2c0b8ca7"
            tid = res.split('table_ts_')[-1].split('"')[0]
            # now we have the tid, we can check task info
            execute(kc=kc, code='%taskinfo -q ts ' + tid)
            res = get_display_data(kc.iopub_channel, 'text/html')
            self.assertTrue(tid in res, 'expect {} in {}'.format(tid, res))
            # there should be two tasks
            lines = subprocess.check_output(['sos', 'status', '-q', 'ts', '-c', '~/docker.yml']).decode().splitlines()
            self.assertGreaterEqual(len(lines), 2)

    def testListHosts(self):
        '''test list hosts using sos status -q'''
        for v in ['0', '1', '3', '4']:
            # ts of type pbs should be in output
            output = subprocess.check_output(['sos', 'remote', 'list', '-c', '~/docker.yml', '-v', v]).decode()
            self.assertTrue('ts' in output)


    def testSyncInputOutput(self):
        '''Test sync input and output with remote host'''
        for i in range(4):
            if os.path.isfile(f'test_{i}.txt'):
                os.remove(f'test_{i}.txt')
            if os.path.isfile(f'test_{i}.bak'):
                os.remove(f'test_{i}.bak')
        import random
        script = SoS_Script('''
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
''')
        wf = script.workflow()
        val = random.randint(1, 10000)
        Base_Executor(wf, args=['--g', str(val)],
            config={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        }).run()
        # now check if
        for i in range(4):
            self.assertTrue(os.path.isfile(f'test_{i}.txt'))
            with open(f'test_{i}.bak') as outf:
                self.assertEqual(outf.read(), f'test_{i}_{val}.bak')
            self.assertTrue(os.path.isfile(f'test_{i}.bak'))
            with open(f'test_{i}.bak') as outf:
                self.assertEqual(outf.read(), f'test_{i}_{val}.bak')

    def testSyncMasterTask(self):
        '''Test sync input and output with remote host with trunksize'''
        for i in range(4):
            if os.path.isfile(f'test_{i}.txt'):
                os.remove(f'test_{i}.txt')
            if os.path.isfile(f'test_{i}.bak'):
                os.remove(f'test_{i}.bak')
        import random
        script = SoS_Script('''
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
''')
        wf = script.workflow()
        val = random.randint(1, 10000)
        Base_Executor(wf, args=['--g', str(val)],
            config={
            'config_file': '~/docker.yml',
            'default_queue': 'ts',
            'sig_mode': 'force',
        }).run()
        # now check if
        for i in range(4):
            self.assertTrue(os.path.isfile(f'test_{i}.txt'))
            with open(f'test_{i}.bak') as outf:
                self.assertEqual(outf.read(), f'test_{i}_{val}.bak')
            self.assertTrue(os.path.isfile(f'test_{i}.bak'))
            with open(f'test_{i}.bak') as outf:
                self.assertEqual(outf.read(), f'test_{i}_{val}.bak')

if __name__ == '__main__':
    unittest.main()

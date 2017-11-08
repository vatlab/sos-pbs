#!/usr/bin/env python3
#
# This file is part of Script of Scripts (SoS), a workflow system
# for the execution of commands and scripts in different languages.
# Please visit https://github.com/vatlab/SOS for more information.
#
# Copyright (C) 2016 Bo Peng (bpeng@mdanderson.org)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import os
import sys
import unittest

from sos.sos_script import SoS_Script
from sos.utils import env
from sos.sos_executor import Base_Executor
from sos.target import file_target
from sos.hosts import Host
import subprocess
import shutil

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
            file_target(f).remove('both')


    @unittest.skipIf(not shutil.which('ts'), "ts command not found")
    def testLocalTS(self):
        #
        if os.path.exists('a.txt'):
            os.remove('a.txt')
        script = SoS_Script('''
[10]
task:
sh:
  echo "I am done" >> a.txt
''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'wait_for_task': True,
                'default_queue': 'local_ts',
                'sig_mode': 'force',
                }).run()
        self.assertTrue(file_target('a.txt').exists())

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testRemoteTS(self):
        if os.path.exists('ar.txt'):
            os.remove('ar.txt')
        with open('remote_ts.sos', 'w') as rt:
            rt.write('''
[10]
task:
sh:
  echo "I am done" >> ar.txt
''')
        ret = subprocess.call('sos run remote_ts -c ~/docker.yml -q ts', shell=True)
        self.assertTrue(ret == 0)
        ret = subprocess.call('sos pull ar.txt -c ~/docker.yml --from ts', shell=True)
        self.assertTrue(ret == 0)
        self.assertTrue(file_target('ar.txt').exists())

if __name__ == '__main__':
    unittest.main()

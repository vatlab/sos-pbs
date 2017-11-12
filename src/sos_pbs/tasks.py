#!/usr/bin/env python3
#
# This file is part of Script of Scripts (sos), a workflow system
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
from sos.utils import env
from sos.eval import cfg_interpolate
from sos.tasks import TaskEngine, loadTask
from sos.pattern import extract_pattern

class PBS_TaskEngine(TaskEngine):
    def __init__(self, agent):
        super(PBS_TaskEngine, self).__init__(agent)
        # we have self.config for configurations
        #
        # job_template
        # submit_cmd
        #
        # => status_cmd (perhaps not needed)
        # => kill_cmd (perhaps not needed)
        if 'job_template' in self.config:
            self.job_template = self.config['job_template'].replace('\r\n', '\n')
        else:
            raise ValueError(f'A job_template is required for queue {self.alias}')

        if 'submit_cmd' not in self.config:
            raise ValueError(f'Missing configuration submit_cmd for queue {self.alias}')
        else:
            self.submit_cmd = self.config['submit_cmd']

        if 'status_cmd' not in self.config:
            raise ValueError(f'Missing configuration status_cmd for queue {self.alias}')
        else:
            self.status_cmd = self.config['status_cmd']

        if 'kill_cmd' not in self.config:
            raise ValueError(f'Missing configuration kill_cmd for queue {self.alias}')
        else:
            self.kill_cmd = self.config['kill_cmd']

    def execute_task(self, task_id):
        #
        if not super(PBS_TaskEngine, self).execute_task(task_id):
            return False

        try:
            return self._prepare_script(task_id)
        except Exception as e:
            env.logger.error(e)
            return False

    def _prepare_script(self, task_id):
        # read the task file and look for runtime info
        #
        task_file = os.path.join(os.path.expanduser('~'), '.sos', 'tasks', self.alias, task_id + '.task')
        params = loadTask(task_file)
        sos_dict = params.sos_dict

        # for this task, we will need walltime, nodes, cores, mem
        # however, these could be fixed in the job template and we do not need to have them all in the runtime
        runtime = self.config
        # we also use saved verbosity and sig_mode because the current sig_mode might have been changed
        # (e.g. in Jupyter) after the job is saved.
        runtime.update({x:sos_dict['_runtime'][x] for x in ('nodes', 'cores', 'mem', 'walltime', 'cur_dir', 'home_dir', 'verbosity', 'sig_mode', 'run_mode') if x in sos_dict['_runtime']})
        if 'name' in sos_dict['_runtime']:
            env.logger.warning("Runtime option name is deprecated. Please use tags to keep track of task names.")
        runtime['task'] = task_id
        # this is also deprecated
        runtime['job_name'] = task_id
        if 'nodes' not in runtime:
            runtime['nodes'] = 1
        if 'cores' not in runtime:
            runtime['cores'] = 1
        # for backward compatibility
        runtime['job_file'] = f'~/.sos/tasks/{task_id}.sh'

        # let us first prepare a task file
        try:
            job_text = cfg_interpolate(self.job_template, runtime)
        except Exception as e:
            raise ValueError(f'Failed to generate job file for task {task_id}: {e}')

        # now we need to write a job file
        job_file = os.path.join(os.path.expanduser('~'), '.sos', 'tasks', self.alias, task_id + '.sh')
        # do not translate newline under windows because the script will be executed
        # under linux/mac
        with open(job_file, 'w', newline='') as job:
            job.write(job_text)

        # then copy the job file to remote host if necessary
        self.agent.send_task_file(job_file)

        if runtime['run_mode'] == 'dryrun':
            try:
                cmd = f'bash ~/.sos/tasks/{task_id}.sh'
                print(self.agent.check_output(cmd))
            except Exception as e:
                raise RuntimeError(f'Failed to submit task {task_id}: {e}')
        else:
            #
            # now we need to figure out a command to submit the task
            try:
                cmd = cfg_interpolate(self.submit_cmd, runtime)
            except Exception as e:
                raise ValueError(f'Failed to generate job submission command from template "{self.submit_cmd}": {e}')
            env.logger.debug(f'submit {task_id}: {cmd}')
            try:
                try:
                    cmd_output = self.agent.check_output(cmd).strip()
                except Exception as e:
                    raise RuntimeError(f'Failed to submit task {task_id}: {e}')

                if 'submit_cmd_output' not in self.config:
                    submit_cmd_output = '{job_id}'
                else:
                    submit_cmd_output = self.config['submit_cmd_output']
                #
                if not '{job_id}' in submit_cmd_output:
                    raise ValueError(
                        f'Option submit_cmd_output should have at least a pattern for job_id, "{submit_cmd_output}" specified.')
                #
                # try to extract job_id from command output
                # let us write an job_id file so that we can check status of tasks more easily
                job_id_file = os.path.join(os.path.expanduser('~'), '.sos', 'tasks', self.alias, task_id + '.job_id')
                with open(job_id_file, 'w') as job:
                    try:
                        res = extract_pattern(submit_cmd_output, [cmd_output.strip()])
                        if 'job_id' not in res or len(res['job_id']) != 1:
                            env.logger.warning(
                                f'Failed to extract job_id from "{cmd_output.strip()}" using pattern "{submit_cmd_output}"')
                            job_id = '000000'
                            job.write(f'job_id: {job_id}\n')
                        else:
                            job_id = res['job_id'][0]
                            # other variables
                            for k,v in res.items():
                                job.write(f'{k}: {v[0]}\n')
                    except Exception as e:
                        env.logger.warning(
                            f'Failed to extract job_id from "{cmd_output.strip()}" using pattern "{submit_cmd_output}"')
                        job_id = '000000'
                        job.write(f'job_id: {job_id}\n')
                # output job id to stdout
                self.notify(f'{task_id} ``submitted`` to {self.alias} with job id {job_id}')
                return True
            except Exception as e:
                raise RuntimeError(f'Failed to submit task {task_id}: {e}')

    def _get_job_id(self, task_id):
        job_id_file = os.path.join(os.path.expanduser('~'), '.sos', 'tasks', self.alias, task_id + '.job_id')
        if not os.path.isfile(job_id_file):
            return {}
        with open(job_id_file) as job:
            result = {}
            for line in job:
                k, v = line.split(':', 1)
                result[k.strip()] = v.strip()
            return result

    def _query_job_status(self, job_id, task_id):
        job_id.update({'task': task_id, 'verbosity': 1})
        cmd = cfg_interpolate(self.status_cmd, job_id)
        return self.agent.check_output(cmd)

    def query_tasks(self, tasks=None, verbosity=1, html=False, **kwargs):
        if verbosity <= 2:
            status_lines = super(PBS_TaskEngine, self).query_tasks(tasks, verbosity, html, **kwargs)
            # there is a change that a job is submitted, but failed before the sos command is executed
            # so we will have to ask the task engine about the submitted jobs #608
            if not html:
                res = ''
                for line in status_lines.split('\n'):
                    if not line.strip():
                        continue
                    fields = line.split('\t')
                    if len(fields) < verbosity:
                        env.logger.error(fields)
                        env.logger.warning(f'Suspicious status line {line}')
                        continue
                    task_id = fields[0]
                    if fields[verbosity] == 'submitted':
                        try:
                            job_id = self._get_job_id(task_id)
                            if not job_id:
                                raise RuntimeError(f'failed to obtain job id for task {task_id}')
                            self._query_job_status(job_id, task_id)
                        except Exception as e:
                            env.logger.trace(f'Failed to query status for task {task_id}: {e}')
                            fields[verbosity] = 'failed'
                    res += '\t'.join(fields) + '\n'
                return res
            else:
                # ID line: <tr><th align="right"  width="30%">ID</th><td align="left">5173b80bf85d3d03153b96f9a5b4d6cc</td></tr>
                task_id = status_lines.split('>ID<', 1)[-1].split('</td',1)[0].split('>')[-1]
                status = status_lines.split('>Status<', 1)[-1].split('</td',1)[0].split('>')[-1]
                if status == 'submitted':
                    try:
                        job_id = self._get_job_id(task_id)
                        if not job_id:
                            raise RuntimeError(f'failed to obtain job id for task {task_id}')
                        self._query_job_status(job_id, task_id)
                    except Exception as e:
                        env.logger.trace(f'Failed to query status for task {task_id}: {e}')
                        status_lines = status_lines.replace('submitted', 'failed', 1)
                return status_lines

        # for more verbose case, we will call pbs's status_cmd to get more accurate information
        status_lines = super(PBS_TaskEngine, self).query_tasks(tasks, 1)
        res = ''
        for line in status_lines.split('\n'):
            if not line.strip():
                continue
            task_id, status = line.split('\t')
            # call query_tasks again for more verbose output
            res += super(PBS_TaskEngine, self).query_tasks([task_id], verbosity, html) + '\n'
            #
            try:
                job_id = self._get_job_id(task_id)
                if not job_id:
                    # no job id file
                    raise RuntimeError(f'failed to obtain job id for task {task_id}')
                res += self._query_job_status(job_id, task_id)
            except Exception as e:
                env.logger.debug(
                    f'Failed to get status of task {task_id} (job_id: {job_id}) from template "{self.status_cmd}": {e}')
        return res

    def kill_tasks(self, tasks, **kwargs):
        # remove the task from SoS task queue, this would also give us a list of
        # tasks on the remote server
        output = super(PBS_TaskEngine, self).kill_tasks(tasks, **kwargs)
        env.logger.trace(f'Output of local kill: {output}')
        # then we call the real PBS commands to kill tasks
        res = ''
        for line in output.split('\n'):
            if not line.strip():
                continue
            task_id, status = line.split('\t')
            res += f'{task_id}\t{status} (old status)\t'

            job_id = self._get_job_id(task_id)
            if not job_id:
                env.logger.debug(f'No job_id for task {task_id}')
                continue
            try:
                job_id.update({'task': task_id})
                cmd = cfg_interpolate(self.kill_cmd, job_id)
                env.logger.debug(f'Running {cmd}')
                res += self.agent.check_output(cmd) + '\n'
            except Exception as e:
                env.logger.debug(
                    f'Failed to kill job {task_id} (job_id: {job_id}) from template "{self.kill_cmd}": {e}')
        return res

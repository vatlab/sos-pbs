#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import subprocess

from sos.utils import env
from sos.eval import cfg_interpolate
from sos.task_engines import TaskEngine
from sos.tasks import TaskFile
from sos.pattern import extract_pattern


class PBS_TaskEngine(TaskEngine):

    def __init__(self, agent):
        super(PBS_TaskEngine, self).__init__(agent)
        # we have self.config for configurations
        #
        # task_template
        # submit_cmd
        #
        # => status_cmd (perhaps not needed)
        # => kill_cmd (perhaps not needed)
        if 'task_template' in self.config:
            self.task_template = self.config['task_template'].replace(
                '\r\n', '\n')
        elif 'job_template' in self.config:
            env.logger.warning('job_template for host configuration is deprecated. Please use task_template instead.')
            self.task_template = self.config['job_template'].replace(
                '\r\n', '\n')
        else:
            raise ValueError(
                f'A task_template is required for queue {self.alias}')

        if 'submit_cmd' not in self.config:
            raise ValueError(
                f'Missing configuration submit_cmd for queue {self.alias}')
        else:
            self.submit_cmd = self.config['submit_cmd']

        if 'status_cmd' not in self.config:
            raise ValueError(
                f'Missing configuration status_cmd for queue {self.alias}')
        else:
            self.status_cmd = self.config['status_cmd']

        if 'kill_cmd' not in self.config:
            raise ValueError(
                f'Missing configuration kill_cmd for queue {self.alias}')
        else:
            self.kill_cmd = self.config['kill_cmd']

    def execute_tasks(self, task_ids):
        #
        if not super(PBS_TaskEngine, self).execute_tasks(task_ids):
            return False

        try:
            for task_id in task_ids:
                if not self._prepare_script(task_id):
                    return False
            return True
        except Exception as e:
            env.logger.error(str(e))
            return False

    def _prepare_script(self, task_id):
        # read the task file and look for runtime info
        #
        task_runtime = TaskFile(task_id).runtime
        # individual task can have its own _runtime in sos_dict
        task_runtime['_runtime'].update(
            TaskFile(task_id).params.sos_dict.get('_runtime', {}))

        # for this task, we will need walltime, nodes, cores, mem
        # however, these could be fixed in the job template and we do not need to have them all in the runtime
        runtime = self.config
        # we also use saved verbosity and sig_mode because the current sig_mode might have been changed
        # (e.g. in Jupyter) after the job is saved.

        runtime.update({
            x: task_runtime['_runtime'][x]
            for x in ('nodes', 'cores', 'mem', 'walltime', 'workdir',
                      'verbosity', 'sig_mode', 'run_mode')
            if x in task_runtime['_runtime']
        })
        # workdir should exist, cur_dir is kept for backward compatibility
        runtime['cur_dir'] = runtime['workdir']
        if 'name' in task_runtime['_runtime']:
            env.logger.warning(
                "Runtime option name is deprecated. Please use tags to keep track of task names."
            )
        runtime['task'] = task_id
        # job_name is recommended because of compatibility with workflow_template
        runtime['job_name'] = task_id
        runtime['command'] = f'sos execute {task_id} -v {runtime["verbosity"]} -s {runtime["sig_mode"]} -m {runtime["run_mode"]}'
        if 'nodes' not in runtime:
            runtime['nodes'] = 1
        if 'cores' not in runtime:
            runtime['cores'] = 1
        # for backward compatibility
        runtime['job_file'] = f'~/.sos/tasks/{task_id}.sh'

        # let us first prepare a task file
        try:
            job_text = cfg_interpolate(self.task_template, runtime)
        except Exception as e:
            raise ValueError(
                f'Failed to generate job file for task {task_id}: {e}')

        # now we need to write a job file
        job_file = os.path.join(
            os.path.expanduser('~'), '.sos', 'tasks', task_id + '.sh')
        # do not translate newline under windows because the script will be executed
        # under linux/mac
        with open(job_file, 'w', newline='') as job:
            job.write(job_text)

        # then copy the job file to remote host if necessary
        self.agent.send_job_file(job_file)

        if runtime['run_mode'] == 'dryrun':
            try:
                cmd = f'bash ~/.sos/tasks/{task_id}.sh'
                print(self.agent.check_output(cmd))
            except Exception as e:
                raise RuntimeError(f'Failed to submit task {task_id}: {e}')
            return
        #
        # now we need to figure out a command to submit the task
        try:
            cmd = cfg_interpolate(self.submit_cmd, runtime)
        except Exception as e:
            raise ValueError(
                f'Failed to generate job submission command from template "{self.submit_cmd}": {e}'
            )
        env.logger.debug(f'submit {task_id}: {cmd}')
        try:
            cmd_output = self.agent.check_output(
                cmd, stderr=subprocess.STDOUT).strip()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f'Failed to submit task {task_id}:\n{e.output.decode()}')
        except Exception as e:
            raise RuntimeError(f'Failed to submit task {task_id}:\n{e}')

        if not cmd_output:
            raise RuntimeError(
                f'Failed to submit task {task_id} with command {cmd}. No output returned.'
            )

        if 'submit_cmd_output' not in self.config:
            submit_cmd_output = '{job_id}'
        else:
            submit_cmd_output = self.config['submit_cmd_output']
        #
        if not '{job_id}' in submit_cmd_output:
            raise ValueError(
                f'Option submit_cmd_output should have at least a pattern for job_id, "{submit_cmd_output}" specified.'
            )

        #
        # try to extract job_id from command output
        # let us write an job_id file so that we can check status of tasks more easily
        job_id_file = os.path.join(
            os.path.expanduser('~'), '.sos', 'tasks', task_id + '.job_id')
        with open(job_id_file, 'w') as job:
            res = extract_pattern(submit_cmd_output, [cmd_output.strip()])
            if 'job_id' not in res or len(
                    res['job_id']) != 1 or res['job_id'][0] is None:
                raise RuntimeError(
                    f'Failed to extract job_id from "{cmd_output.strip()}" using pattern "{submit_cmd_output}"'
                )
            else:
                job_id = res['job_id'][0]
                # other variables
                for k, v in res.items():
                    job.write(f'{k}: {v[0]}\n')
        try:
            # Send job id files to remote host so that
            # 1. the job could be properly killed (with job_id) on remote host (not remotely)
            # 2. the job status could be perperly probed in case the job was not properly submitted (#911)
            self.agent.send_job_file(job_id_file)
            # output job id to stdout
            env.logger.info(
                f'{task_id} ``submitted`` to {self.alias} with job id {job_id}')
            return True
        except Exception as e:
            raise RuntimeError(f'Failed to submit task {task_id}: {e}')

    def _get_job_id(self, task_id):
        job_id_file = os.path.join(
            os.path.expanduser('~'), '.sos', 'tasks', task_id + '.job_id')
        if not os.path.isfile(job_id_file):
            return {}
        with open(job_id_file) as job:
            result = {}
            for line in job:
                k, v = line.split(':', 1)
                result[k.strip()] = v.strip()
            return result


#     def _query_job_status(self, job_id, task_id):
#         job_id.update({'task': task_id, 'verbosity': 1})
#         cmd = cfg_interpolate(self.status_cmd, job_id)
#         return self.agent.check_output(cmd)
#
#     def query_tasks(self, tasks=None, verbosity=1, html=False, **kwargs):
#         if verbosity <= 3:
#             status_lines = super(PBS_TaskEngine, self).query_tasks(tasks, verbosity, html, **kwargs)
#             # there is a change that a job is submitted, but failed before the sos command is executed
#             # so we will have to ask the task engine about the submitted jobs #608
#             if not html:
#                 res = ''
#                 for line in status_lines.split('\n'):
#                     if not line.strip():
#                         continue
#                     fields = line.split('\t')
#                     if len(fields) < verbosity:
#                         env.logger.error(fields)
#                         env.logger.warning(f'Suspicious status line {line}')
#                         continue
#                     task_id = fields[0]
#                     if fields[verbosity] == 'submitted':
#                         try:
#                             job_id = self._get_job_id(task_id)
#                             if not job_id:
#                                 raise RuntimeError(f'failed to obtain job id for task {task_id}')
#                             self._query_job_status(job_id, task_id)
#                         except Exception as e:
#                             env.logger.trace(f'Failed to query status for task {task_id}: {e}')
#                             fields[verbosity] = 'failed'
#                     res += '\t'.join(fields) + '\n'
#                 return res
#             else:
#                 # ID line: <tr><th align="right"  width="30%">ID</th><td align="left">5173b80bf85d3d03153b96f9a5b4d6cc</td></tr>
#                 task_id = status_lines.split('>ID<', 1)[-1].split('</td',1)[0].split('>')[-1]
#                 status = status_lines.split('>Status<', 1)[-1].split('</td',1)[0].split('>')[-1]
#                 if status == 'submitted':
#                     try:
#                         job_id = self._get_job_id(task_id)
#                         if not job_id:
#                             raise RuntimeError(f'failed to obtain job id for task {task_id}')
#                         self._query_job_status(job_id, task_id)
#                     except Exception as e:
#                         env.logger.trace(f'Failed to query status for task {task_id}: {e}')
#                         status_lines = status_lines.replace('submitted', 'failed', 1)
#                 return status_lines
#
#         # for more verbose case, we will call pbs's status_cmd to get more accurate information
#         status_lines = super(PBS_TaskEngine, self).query_tasks(tasks, 1)
#         res = ''
#         for line in status_lines.split('\n'):
#             if not line.strip():
#                 continue
#             task_id, status = line.split('\t')
#             # call query_tasks again for more verbose output
#             res += super(PBS_TaskEngine, self).query_tasks([task_id], verbosity, html) + '\n'
#             #
#             try:
#                 job_id = self._get_job_id(task_id)
#                 if not job_id:
#                     # no job id file
#                     raise RuntimeError(f'failed to obtain job id for task {task_id}')
#                 res += self._query_job_status(job_id, task_id)
#             except Exception as e:
#                 env.logger.debug(
#                     f'Failed to get status of task {task_id} (job_id: {job_id}) from template "{self.status_cmd}": {e}')
#         return res

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
            res += f'{task_id}\t{status}\t'
            # only run kill_cmd on killed or aborted jobs
            if status.strip() not in ('killed', 'aborted'):
                res += '.\n'
                continue

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
                    f'Failed to kill job {task_id} (job_id: {job_id}) from template "{self.kill_cmd}": {e}'
                )
        return res

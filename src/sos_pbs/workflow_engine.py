#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import subprocess

from sos.utils import env
from sos.eval import cfg_interpolate
from sos.workflow_engines import WorkflowEngine
from sos.pattern import extract_pattern


class PBS_WorkflowEngine(WorkflowEngine):

    def __init__(self, agent):
        super(PBS_WorkflowEngine, self).__init__(agent)
        # we have self.config for configurations
        #
        # workflow_template
        if 'workflow_template' in self.config:
            self.workflow_template = self.config['workflow_template'].replace(
                '\r\n', '\n')
        else:
            raise ValueError(
                f'A workflow_template is required for queue {self.alias}')

        if 'submit_cmd' not in self.config:
            raise ValueError(
                f'Missing configuration submit_cmd for queue {self.alias}')
        else:
            self.submit_cmd = self.config['submit_cmd']

    def execute_workflow(self, filename, command, **template_args):
        #
        # calling super execute_workflow would set cleaned versions
        # of self.filename, self.command, and self.template_args
        if not super(PBS_WorkflowEngine, self).execute_workflow(
                filename, command, **template_args):
            env.log_to_file(
                'WORKFLOW',
                f'Failed to prepare workflow with command "{command}"')
            return False

        self.expand_template()

        # then copy the job file to remote host if necessary
        self.agent.send_job_file(self.job_file, dir='workflows')

        if 'run_mode' in self.config and self.config['run_mode'] == 'dryrun':
            try:
                cmd = f'bash ~/.sos/workflows/{self.job_name}.sh'
                print(self.agent.check_output(cmd))
            except Exception as e:
                raise RuntimeError(
                    f'Failed to submit workflow {self.job_name}: {e}')
            return
        #
        self.template_args['job_file'] = f'~/.sos/workflows/{self.job_name}.sh'
        # now we need to figure out a command to submit the workflow
        try:
            cmd = cfg_interpolate(self.submit_cmd, self.template_args)
        except Exception as e:
            raise ValueError(
                f'Failed to generate job submission command from template "{self.submit_cmd}": {e}'
            )
        env.logger.debug(f'submit {self.job_name}: {cmd}')
        try:
            cmd_output = self.agent.check_output(cmd).strip()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f'Failed to submit workflow {self.job_name}:\n{e.output.decode()}'
            )
        except Exception as e:
            raise RuntimeError(
                f'Failed to submit workflow {self.job_name}:\n{e}')

        if not cmd_output:
            raise RuntimeError(
                f'Failed to submit workflow {self.job_name} with command {cmd}. No output returned.'
            )

        if 'submit_cmd_output' not in self.config:
            submit_cmd_output = '{job_id}'
        else:
            submit_cmd_output = self.config['submit_cmd_output']
        #
        if '{job_id}' not in submit_cmd_output:
            raise ValueError(
                f'Option submit_cmd_output should have at least a pattern for job_id, "{submit_cmd_output}" specified.'
            )

        #
        # try to extract job_id from command output
        # let us write an job_id file so that we can check status of workflows more easily
        job_id_file = os.path.join(
            os.path.expanduser('~'), '.sos', 'workflows',
            self.job_name + '.job_id')
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
            self.agent.send_job_file(job_id_file, dir='workflows')
            # output job id to stdout
            env.logger.info(
                f'{self.job_name} ``submitted`` to {self.alias} with job id {job_id}'
            )
            return True
        except Exception as e:
            raise RuntimeError(
                f'Failed to submit workflow {self.job_name}: {e}')

    def _get_job_id(self, job_name):
        job_id_file = os.path.join(
            os.path.expanduser('~'), '.sos', 'workflows', job_name + '.job_id')
        if not os.path.isfile(job_id_file):
            return {}
        with open(job_id_file) as job:
            result = {}
            for line in job:
                k, v = line.split(':', 1)
                result[k.strip()] = v.strip()
            return result

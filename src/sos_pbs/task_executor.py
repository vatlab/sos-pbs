#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import zmq

from threading import Event

from sos.task_executor import BaseTaskExecutor
from sos.controller import (Controller, connect_controllers,
                            disconnect_controllers, create_socket, close_socket,
                            request_answer_from_controller,
                            send_message_to_controller)
from sos.utils import env, get_localhost_ip
from sos.messages import decode_msg

from collections.abc import Sequence


class PBS_TaskExecutor(BaseTaskExecutor):

    def __init__(self, *args, **kwargs):
        super(PBS_TaskExecutor, self).__init__(*args, **kwargs)

    def _parse_num_workers(self, num_workers):
        # return number of nodes and workers
        if isinstance(num_workers, Sequence):
            if len(num_workers) >= 1:
                val = num_workers[0]
                if ':' in val:
                    val = val.rsplit(':', 1)[-1]
                n_workers = int(val.rsplit(':', 1)[-1])
                return len(num_workers), None if n_workers <= 0 else n_workers
            else:
                return None, None
        elif isinstance(num_workers, str):
            if ':' in num_workers:
                num_workers = num_workers.rsplit(':', 1)[-1]
            n_workers = int(num_workers.rsplit(':', 1)[-1])
            return 1, None if n_workers <= 0 else n_workers
        elif isinstance(num_workers, int) and num_workers >= 1:
            return 1, num_workers
        elif num_workers is None:
            return None, None
        else:
            raise RuntimeError(
                f"Unacceptable value for parameter trunk_workers {num_workers}")

    def execute_master_task(self, task_id, params, master_runtime, sig_content):

        n_nodes, n_procs = self._parse_num_workers(env.config['worker_procs'])

        # regular trunk_workers = ?? (0 was used as default)
        num_workers = params.sos_dict['_runtime']['num_workers']
        if (isinstance(num_workers, int) and num_workers > 1) or n_nodes == 1:
            return super(PBS_TaskExecutor,
                         self).execute_master_task(task_id, params,
                                                   master_runtime, sig_content)

        # case :
        #
        # 1. users do not speicy trunk_workers (num_workers is None or 0)
        #    and -j is hard coded, ok.
        # we use all workers specified by -j, which is OK
        #
        # 2. users have nodes for multi-node tasks, in this case trunk_workers will be None
        #  and it will be a single task, executed by base executor.
        #
        # 3. users specify trunk_workers, should should match what we have or change -j
        if num_workers is not None:  # should be a sequence
            if not num_workers:
                num_workers = env.config['worker_procs']
            elif len(num_workers) != n_nodes:
                env.logger.warning(
                    f'task options trunk_workers={num_workers} is inconsistent with command line option -j {env.config["worker_procs"]}'
                )

        #
        # Now, we will need to start a controller and multiple worker...
        #
        # used for self._collect_subtask_ids to determine master stdout and stderr
        self.master_stdout = os.path.join(
            os.path.expanduser('~'), '.sos', 'tasks', task_id + '.out')
        self.master_stderr = os.path.join(
            os.path.expanduser('~'), '.sos', 'tasks', task_id + '.err')

        if os.path.exists(self.master_stdout):
            open(self.master_stdout, 'w').close()
        if os.path.exists(self.master_stderr):
            open(self.master_stderr, 'w').close()

        env.zmq_context = zmq.Context()

        # control panel in a separate thread, connected by zmq socket
        ready = Event()
        self.controller = Controller(ready)
        self.controller.start()
        # wait for the thread to start with a signature_req saved to env.config
        ready.wait()

        connect_controllers(env.zmq_context)

        try:

            # start a result receving socket
            self.result_pull_socket = create_socket(env.zmq_context, zmq.PULL,
                                                    'substep result collector')
            local_ip = get_localhost_ip()
            port = self.result_pull_socket.bind_to_random_port(
                f'tcp://{local_ip}')
            env.config['sockets'][
                'result_push_socket'] = f'tcp://{local_ip}:{port}'

            # send tasks to the controller
            results = []
            for sub_id, sub_params in params.task_stack:
                if hasattr(params, 'common_dict'):
                    sub_params.sos_dict.update(params.common_dict)
                sub_runtime = {
                    x: master_runtime.get(x, {}) for x in ('_runtime', sub_id)
                }
                sub_sig = {sub_id: sig_content.get(sub_id, {})}

                # submit tasks
                send_message_to_controller([
                    'task',
                    dict(
                        task_id=sub_id,
                        params=sub_params,
                        runtime=sub_runtime,
                        sig_content=sub_sig,
                        config=env.config,
                        quiet=True)
                ])

            for idx in range(len(params.task_stack)):
                res = decode_msg(self.result_pull_socket.recv())
                try:
                    self._append_subtask_outputs(res)
                except Exception as e:
                    env.logger.warning(f'Failed to copy result of subtask: {e}')
                results.append(res)
            succ = True
        except Exception as e:
            env.logger.error(f'Failed to execute master task {task_id}: {e}')
            succ = False
        finally:
            # end progress bar when the master workflow stops
            close_socket(self.result_pull_socket)
            env.log_to_file('EXECUTOR', f'Stop controller from {os.getpid()}')
            request_answer_from_controller(['done', succ])
            env.log_to_file('EXECUTOR', 'disconntecting master')
            self.controller.join()
            disconnect_controllers(env.zmq_context if succ else None)

        return self._combine_results(task_id, results)

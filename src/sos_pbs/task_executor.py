#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

from sos.task_executor import BaseTaskExecutor


class PBSTaskExecutor(BaseTaskExecutor):

    def __init__(self, *args, **kwargs):
        super(PBSTaskExecutor, self).__init__(*args, **kwargs)

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
            raise RuntimeError(f"Unacceptable value for parameter trunk_workers {num_workers}")

    def execute_master_task(self, task_id, params, master_runtime, sig_content):

        n_nodes, n_procs = self._parse_num_workers(self.worker_procs)

        # regular trunk_workers = ?? (0 was used as default)
        if (isinstance(params.num_workers, int) and params.num_workers > 1) or n_nodes == 1:
            return super(PBSTaskExecutor, self).execute_master_task(task_id, params, master_runtime, sig_content)

        # case :
        #
        # 1. users do not speicy trunk_workers (params.num_workers is None or 0)
        #    and -j is hard coded, ok.
        # we use all workers specified by -j, which is OK
        #
        # 2. users have nodes for multi-node tasks, in this case trunk_workers will be None
        #  and it will be a single task, executed by base executor.
        #
        # 3. users specify trunk_workers, should should match what we have or change -j
        if params.num_workers is not None: # should be a sequence
            if not params.num_workers):
                params.num_workers = self.worker_proces
            elif len(params.num_workers) != n_nodes:
                env.logger.warning(f'task options trunk_workers={params.num_workers} is inconsistent with command line option -j {self.worker_procs}')

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

        # if this is a master task, calling each sub task
        if params.num_workers > 1:
            from multiprocessing.pool import Pool
            p = Pool(params.num_workers)
            results = []
            for sub_id, sub_params in params.task_stack:
                if hasattr(params, 'common_dict'):
                    sub_params.sos_dict.update(params.common_dict)
                sub_runtime = {
                    x: master_runtime.get(x, {}) for x in ('_runtime', sub_id)
                }
                sub_sig = {sub_id: sig_content.get(sub_id, {})}
                results.append(
                    p.apply_async(
                        self._execute_task,
                        (sub_id, sub_params, sub_runtime, sub_sig, True),
                        callback=self._append_subtask_outputs))
            for idx, r in enumerate(results):
                results[idx] = r.get()
            p.close()
            p.join()
        else:
            results = []
            for sub_id, sub_params in params.task_stack:
                if hasattr(params, 'common_dict'):
                    sub_params.sos_dict.update(params.common_dict)
                sub_runtime = {
                    x: master_runtime.get(x, {}) for x in ('_runtime', sub_id)
                }
                sub_sig = {sub_id: sig_content.get(sub_id, {})}
                res = self.execute_single_task(sub_id, sub_params, sub_runtime,
                                               sub_sig, True)
                try:
                    self._append_subtask_outputs(res)
                except Exception as e:
                    env.logger.warning(
                        f'Failed to copy result of subtask {tid}: {e}')
                results.append(res)
        return self._combine_results(task_id, results)

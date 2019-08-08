#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

from sos.task_executor import BaseTaskExecutor


class PBSTaskExecutor(BaseTaskExecutor):

    def __init__(self, *args, **kwargs):
        super(PBSTaskExecutor, self).__init__(*args, **kwargs)

    def execute_master_task(self, task_id, params, master_runtime, sig_content):

        # regular trunk_workers = ??
        if isinstance(params.num_workers, int):
            return super(PBSTaskExecutor, self).execute_master_task(task_id, params, master_runtime, sig_content)

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

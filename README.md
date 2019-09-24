[![PyPI version](https://badge.fury.io/py/sos-pbs.svg)](https://badge.fury.io/py/sos-pbs)
[![Build Status](https://travis-ci.org/vatlab/sos-pbs.svg?branch=master)](https://travis-ci.org/vatlab/sos-pbs)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/46ae533bdb614265a9fe78f077e54d87)](https://www.codacy.com/app/BoPeng/sos-pbs?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=vatlab/sos-pbs&amp;utm_campaign=Badge_Grade)
[![Coverage Status](https://coveralls.io/repos/github/vatlab/sos-pbs/badge.svg)](https://coveralls.io/github/vatlab/sos-pbs)

# PBS task engine for SoS Workflow

A workflow engine for SoS Workflow that allows the submission of tasks to PBS systems. Currently supported
cluster systems include PBS, PBS Pro, Torque, SLURM, IBM LSF, and Sun Grid. Other cluster systems are
probably also supported but we do not have access to one and cannot test sos-pbs against it.

## Installation

`sos-pbs` is needed for `sos` to submit tasks to PBS-based environments, but is not needed for the
execution of tasks on such environments. That is to say, `sos-pbs` is not needed on a remote cluster
system (whereas `sos` is needed there).

### Conda-based installation

If you are using a conda environment, use command

```
conda install sos-pbs -c conda-forge
```
to install `sos-pbs` and `sos` to your system.

### Pip-based installation

If you are not using conda, you can install `sos-pbs` with command

```
pip install sos-pbs
```

## Documentation

Please refer to [SoS Homepage](http://vatlab.github.io/SoS/) and [SoS Workflow documentation](https://vatlab.github.io/sos-docs/workflow.html#content) for details.


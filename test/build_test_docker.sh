#!/usr/bin/bash

#
# Make sure we have local public key
#
instance=$(docker ps | grep test_sos)
if [ "${instance}" != "" ]
then
    #echo "sshd is already running"
    #exit
    docker stop test_sos
fi

docker rm test_sos
docker rmi eg_sshd

# this will create ~/.ssh/id_rsa.pub and ~/.ssh/id_rsa
[ -f ~/.ssh/id_rsa.pub ] || ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa
# copy the public key here
cp ~/.ssh/id_rsa.pub authorized_keys

# create a docker file
#
cat > Dockerfile << 'HERE'
FROM python:3.6

RUN apt-get update && apt-get install -y openssh-server rsync task-spooler
RUN mkdir /var/run/sshd
RUN echo 'root:screencast' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

RUN [ -d /root/.ssh ] || mkdir -p /root/.ssh
ADD authorized_keys /root/.ssh/authorized_keys

# install sos on the remote host
RUN  pip install spyder jedi notebook nbconvert nbformat pyyaml psutil tqdm
RUN  pip install fasteners pygments ipython ptpython networkx pydotplus
RUN  pip install git+https://github.com/vatlab/sos.git
RUN  pip install git+https://github.com/vatlab/sos-notebook.git
RUN  pip install git+https://github.com/vatlab/sos-bash.git
RUN  python -m sos_notebook.install

ARG  SHA=LATEST
RUN  SHA=$SHA git clone http://github.com/vatlab/sos-pbs sos-pbs
RUN  cd sos-pbs && pip install -e .

RUN  echo "export TS_SLOTS=10" >> /root/.bash_profile

RUN  mkdir $HOME/.sos
RUN  echo "localhost: ts" > $HOME/.sos/hosts.yml
RUN  echo "hosts:" >> $HOME/.sos/hosts.yml
RUN  echo "    ts:" >> $HOME/.sos/hosts.yml
RUN  echo "        description: task spooler on the docker machine" >> $HOME/.sos/hosts.yml
RUN  echo "        address: localhost" >> $HOME/.sos/hosts.yml
RUN  echo "        queue_type: pbs" >> $HOME/.sos/hosts.yml
RUN  echo "        status_check_interval: 5" >> $HOME/.sos/hosts.yml
RUN  echo "        task_template: |" >> $HOME/.sos/hosts.yml
RUN  echo "            #!/bin/bash" >> $HOME/.sos/hosts.yml
RUN  echo "            cd {workdir}" >> $HOME/.sos/hosts.yml
RUN  echo "            sos execute {task} -v {verbosity} -s {sig_mode} {'--dryrun' if run_mode == 'dryrun' else ''}" >> $HOME/.sos/hosts.yml
RUN  echo "        max_running_jobs: 100" >> $HOME/.sos/hosts.yml
RUN  echo "        submit_cmd: tsp -L {task} sh {job_file}" >> $HOME/.sos/hosts.yml
RUN  echo "        status_cmd: tsp -s {job_id}" >> $HOME/.sos/hosts.yml
RUN  echo "        kill_cmd: tsp -r {job_id}" >> $HOME/.sos/hosts.yml

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
HERE

#

#
# Build the docker image, but we will force docker to use latest github commit
#
SHA=$(curl -s 'https://api.github.com/repos/vatlab/sos-pbs/commits' | grep sha | head -1 | cut -d\" -f4)
echo "COMMIT SHA $SHA"
docker build --build-arg SHA=$SHA -t eg_sshd .

#
# start docker image
docker run -d -P --env TS_SLOTS=10 --name test_sos eg_sshd

# get the port
PORT22=$(docker port test_sos 22 | cut -f2 -d:)

# add the docker machine to known_hosts so that sos will not be
# prompt with the message "are you sure you want to connect"?

ssh  -o 'StrictHostKeyChecking no' -p $PORT22 root@localhost exit

# write a host file
cat > ~/docker.yml << HERE
localhost: localhost
remote_user: root
limited:
    max_cores: 1
    max_mem: 1G

hosts:
    localhost:
        description: localhost
        address: localhost
        paths:
            home: $HOME
    docker:
        address: "{remote_user}@localhost"
        port: $PORT22
        paths:
            home: "/{remote_user}"
    local_limited:
        based_on:
           - hosts.localhost
           - limited
        max_walltime: 10
    docker_limited:
        based_on:
           - hosts.docker
           - limited
        max_walltime: 10
    local_rq:
        based_on: hosts.localhost
        description: rq server with worker
        queue_type: rq
        redis_host: localhost
        redis_port: 6379
        queue: high
    ts:
        description: task spooler on the docker machine
        based_on: hosts.docker
HERE

# this part is not interpolated
cat >> ~/docker.yml << 'HERE'
        queue_type: pbs
        status_check_interval: 5
        task_template: |
            #!/bin/bash
            sos execute {task} -v {verbosity} -s {sig_mode} {'--dryrun' if run_mode == 'dryrun' else ''}
        max_running_jobs: 100
        submit_cmd: tsp -L {task} sh {job_file}
        status_cmd: tsp -s {job_id}
        kill_cmd: tsp -r {job_id}
    local_ts:
        description: task spooler on the docker machine
        address: localhost
        queue_type: pbs
        status_check_interval: 5
        task_template: |
            #!/bin/bash
            sos execute {task} -v {verbosity} -s {sig_mode} {'--dryrun' if run_mode == 'dryrun' else ''}
        max_running_jobs: 100
        submit_cmd: tsp -L {task} sh {job_file}
        status_cmd: tsp -s {job_id}
        kill_cmd: tsp -r {job_id}
HERE

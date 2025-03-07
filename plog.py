# -*- coding: utf-8 -*-
#
#【plog】
#
# Overview:
#      This script uses Python's joblib to perform SSH on hosts specified in the `hostfile`
#      and execute commands listed in the cmds file. It retrieves the execution results back
#      to the main process and saves them in a designated directory.
#
# usage:
#      python3 plog.py --host <host file>
#                      --cmds <cmds file>
#                      --nproc <number of processes>
#                      --output_dir <output_dir>
#     hostfile:
#       A file containing hosts. Each line should be formatted as:
#       ```
#        Optional host name<TAB>hostname or IP
#        ```
#        An SSH connection will be executed for each entry specified in the hostfile.
#
#     cmds:
#       The commands to be executed via SSH on the remote hosts. Each line should be
#       formatted as "Optional command name<TAB>command".
#
#     nproc:
#       The number of parallel processes to use with joblib.
#
#     output_dir:
#       The directory path where the execution results will be saved.
#       Each command's result will be output as
#       ```
#       hostname/commandname_executiontime.txt
#       ```
#
import os
import sys
import re
import paramiko
import socket
import argparse
import logging
from datetime import datetime
from joblib import Parallel, delayed


logging.basicConfig(level=logging.INFO,format='[%(asctime)s] %(levelname)s -- : %(message)s')
logger = logging.getLogger(__name__)


def exec_cmd(cmd, host, timeout=30):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    cmd_name = cmd["name"]
    cmd_str = cmd["cmd"]

    host_name = host["name"]
    host_str = host["host"]

    ret = {
        "host": host_name,
        "cmd": cmd_name,
        "stdout": None,
        "stderr": None
    }
    
    try:
        client.connect(hostname=host_str, timeout=timeout)
        _, stdout, stderr = client.exec_command(cmd_str, timeout=timeout)
        
        output = stdout.read().decode("utf-8")
        error = stderr.read().decode("utf-8")

        if error.strip():
            ret["stderr"] = error.strip
        else:
            ret["stdout"] = output
                        
    except (paramiko.ssh_exception.SSHException, paramiko.ssh_exception.AuthenticationException, socket.timeout) as e:
        ret["stderr"] = e

    finally:
        client.close()
        
    return ret

def load_hostfile(input):
    hosts = []
    with open(input, mode="r", encoding="utf-8") as fin:
        for line in fin:
            line = line.rstrip("\n")
            # comment line
            if re.search(r'^#', line): continue
            # name, hostname
            elems = line.split("\t")
            hosts.append({"name": elems[0], "host": elems[1]})

    return hosts

def load_cmds(input):
    cmds= []
    with open(input, mode="r", encoding="utf-8") as fin:
        for line in fin:
            line = line.rstrip("\n")
            # comment line
            if re.search(r'^#', line): continue
            # name, cmd
            elems = line.split("\t")
            cmds.append({"name": elems[0], "cmd": elems[1]})

    return cmds

def dump(file, output):
    with open(file, mode="w", encoding="utf-8") as fout:
        fout.write(output+"\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default=None)
    parser.add_argument('--cmds', type=str, default=None)
    parser.add_argument('--nproc', type=int, default=4)
    parser.add_argument('--output_dir', type=str)
    
    args = parser.parse_args()

    # define hosts
    hosts = load_hostfile(args.host)
    nproc = args.nproc
    cmds = load_cmds(args.cmds)
    output_dir = args.output_dir
    
    # start time
    time_str = datetime.now().strftime("%Y%m%d_%H%M")

    # The commands are executed sequentially, while the execution across hosts is parallelized.
    for cmd in cmds:
        rets = Parallel(n_jobs=nproc, backend="threading")(delayed(exec_cmd)(cmd, host) for host in hosts)
        rets = sorted(rets, key=lambda d: d["host"])
        
        for ret in rets:
            host = ret["host"]
            if ret["stderr"] is not None:
                logger.error(f"[error] {cmd} {host}")
                continue

            output = os.path.join(output_dir, host)
            os.makedirs(output, exist_ok=True)
            log_file = os.path.join(output, ret["cmd"] + f"_{time_str}.log")
            dump(log_file, ret["stdout"])


if __name__ == '__main__':
    main()

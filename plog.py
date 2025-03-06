#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#【plog】
#
# 概要:
#      python のjoblib を使って`hostfile`で指定されたホストに`ssh`を実施し
#      cmdsファイルで指定されたコマンドを実行し、実行結果をメインプロセス側に戻し
#      所定のディレクトリにコマンドの実行結果を保存する
#
# usage:
#      python3 plog.py --host <host file>
#                      --cmds <cmds file>
#                      --nproc <number of process>
#                      --output_dir <output_dir>
#
#     hostfile: ホストファイル,ホストの名前(任意)<TAB>hostname or IP を複数行で書く
#               ホストファイルに指定された分だけsshを実施する
#     cmds: ssh 先で実施するコマンド。こちらもコマンド名(任意)<TAB>cmd で書く
#     nproc: joblibで実行時の同時並列数
#     output_dir: 実行結果を保存するディレクトリパス
#                 各cmd の結果は `ホスト名/コマンド名_実行時刻.txt` で出力することにするぞい
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
            # 正常な結果を保存(raw なtext)
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
    # 実行するコマンドをファイルで設定する
    # コマンドラインでいれるとめんどくさい
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
        
    hosts = load_hostfile(args.host)  # ssh するホストファイルを設定
    nproc = args.nproc
    cmds = load_cmds(args.cmds)
    output_dir = args.output_dir

    print(hosts)
    print(cmds)
    
    
    # 実行時間
    time_str = datetime.now().strftime("%Y%m%d_%H%M")

    # 実行コマンドは直列で、実行ホストを並列化
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

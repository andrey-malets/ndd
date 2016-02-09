#!/usr/bin/env python

import argparse
import os
import socket
import subprocess
import sys
import warnings

def add_non_required_options(parser):
    parser.add_argument('-B', metavar='BUFFER', help='buffer size')
    parser.add_argument('-b', metavar='BLOCK', help='block size')
    parser.add_argument('-t', metavar='TIMEOUT', help='epoll_wait() timeout')
    parser.add_argument('-p', metavar='PORT', default='3634',
                        help='alternate port for communication (default: 3634)')

def put_non_required_options(args, cmdline):
    vargs = vars(args)
    for name in ['B', 'b', 't']:
        if vargs.get(name) is not None:
            cmdline += ['-{}'.format(name), str(vargs[name])]

def add_common_options(parser):
    add_non_required_options(parser)
    parser.add_argument(
        '-s', metavar='SRC', help='source machine', required=True)
    parser.add_argument(
        '-o', metavar='OUTPUT', help='output on destination', required=True)

def add_master_options(parser):
    add_common_options(parser)
    parser.add_argument(
        '-i', metavar='INPUT', help='input file on source', required=True)
    parser.add_argument(
        '-D', metavar='PARTITION', help='destination partition')
    parser.add_argument(
        '-d', metavar='DEST', help='destination machine(s)', action='append')

def add_slave_options(parser):
    add_common_options(parser)
    parser.add_argument('-S', metavar='SLAVE_SPEC', help='slaves configuration')

def get_master_parser():
    parser = argparse.ArgumentParser('run ndd with SLURM')
    add_master_options(parser)
    return parser

def get_slave_parser():
    parser = argparse.ArgumentParser('run ndd with SLURM (slave mode)')
    add_slave_options(parser)
    return parser

def get_master_ndd_cmd(args):
    cmd = ['ndd', '-i', args.i, '-s', '{}:{}'.format(args.s, args.p)]
    put_non_required_options(args, cmd)
    return cmd

def get_slave_ndd_cmd(args, env):
    cmd = ['ndd', '-o', args.o]
    slaves = args.S.split(',')
    current_host = socket.gethostname()
    if current_host in slaves:
        index = slaves.index(current_host)
    else:
        prefix_occurrences =\
            [x for x in slaves if current_host.startswith(x)]
        if len(prefix_occurrences) == 1:
            index = slaves.index(prefix_occurrences[0])
            warnings.warn('matching by prefix: {}'.format(prefix_occurrences[0]))
        else:
            raise IndexError('{} is not in slaves list'.format(current_host))
    source = args.s if index == 0 else slaves[index-1]
    cmd += ['-r', '{}:{}'.format(source, args.p)]
    if index != len(slaves) - 1:
        current_slave = slaves[index]
        cmd += ['-s', '{}:{}'.format(current_slave, args.p)]
    else:
        current_slave = None
    put_non_required_options(args, cmd)
    return cmd

def get_slave_cmd(args, spec):
    cmd = [os.path.abspath(__file__),
           '-S', spec, '-s', args.s, '-o', args.o, '-p', args.p]
    put_non_required_options(args, cmd)
    return cmd

def get_idle_nodes(partition):
    assert partition
    res = subprocess.check_output(
        ['sinfo', '-p', partition, '-t', 'idle', '-h', '-o' '%n'])
    return res.strip().split('\n')

def get_slaves(args):
    if args.D is not None:
        return get_idle_nodes(args.D)
    elif args.d is not None:
        return args.d
    else:
        raise ValueError('one if -D or -d must be specified')

def get_srun_cmd(args):
    SRUN = ['srun', '-D', '/', '-K', '-q']
    slaves = get_slaves(args)
    spec = ','.join(slaves)
    cmd = SRUN + \
        ['-N', str(len(slaves)), '-w', spec] + \
        get_slave_cmd(args, spec)
    return cmd

def wait(procs):
    def kill():
        for proc in procs.itervalues():
            if proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait()
                except:
                    pass

    for _ in procs:
        (pid, status) = os.wait()
        assert pid in procs
        proc = procs[pid]
        assert proc.wait() is not None
        if status != 0 or proc.returncode != 0:
            kill()
            return 1
    return 0

def run_master(args):
    cmds = [get_master_ndd_cmd(args), get_srun_cmd(args)]
    procs = {proc.pid: proc for proc in map(subprocess.Popen, cmds)}
    return wait(procs)

def run_slave(args, env):
    return subprocess.call(get_slave_ndd_cmd(args, env))

def main(raw_args, env):
    if len(raw_args) > 0 and raw_args[0] == '-S':
        return run_slave(get_slave_parser().parse_args(raw_args), env)
    else:
        return run_master(get_master_parser().parse_args(raw_args))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:], os.environ))

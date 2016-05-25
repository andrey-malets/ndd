#!/usr/bin/env python

import argparse
import os
import socket
import subprocess
import sys
import warnings


def add_non_required_options(parser):
    parser.add_argument('-n', metavar='NDD', help='path to ndd', default='ndd')
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
    parser.add_argument(
        '-H', action='store_true', help='use ssh, not SLURM')
    parser.add_argument(
        '-r', action='store_true', help='specify that input is a directory')


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
    parser.add_argument(
        '-c', metavar='CURRENT_SLAVE', help='slave specification')


def get_master_parser():
    parser = argparse.ArgumentParser('run ndd with SLURM')
    add_master_options(parser)
    return parser


def get_slave_parser():
    parser = argparse.ArgumentParser('run ndd with SLURM (slave mode)')
    add_slave_options(parser)
    return parser


def get_master_input_args(args, read_fd):
    if read_fd:
        cmd = ['-I', '/dev/fd/{}'.format(read_fd)]
    else:
        cmd = ['-i', args.i]
    return cmd


def get_slave_output_args(args, write_fd):
    if write_fd:
        cmd = ['-O', '/dev/fd/{}'.format(write_fd)]
    else:
        cmd = ['-o', args.o]
    return cmd


def get_master_ndd_cmd(args, read_fd=None):
    cmd = [args.n]
    cmd += get_master_input_args(args, read_fd)
    cmd += ['-s', '{}:{}'.format(args.s, args.p)]
    put_non_required_options(args, cmd)
    return cmd


def get_slave_ndd_cmd(args, env, write_fd=None):
    cmd = [args.n]
    cmd += get_slave_output_args(args, write_fd)
    slaves = args.S.split(',')
    current_host = socket.gethostname()
    if current_host in slaves:
        index = slaves.index(current_host)
    else:
        prefix_occurrences =\
            [x for x in slaves if current_host.startswith(x)]
        if len(prefix_occurrences) == 1:
            index = slaves.index(prefix_occurrences[0])
            warnings.warn(
                'matching by prefix: {}'.format(prefix_occurrences[0]))
        else:
            raise IndexError('{} is not in slaves list'.format(current_host))
    source = args.s if index == 0 else slaves[index-1]
    cmd += ['-r', '{}:{}'.format(source, args.p)]
    if index != len(slaves) - 1:
        current_slave = slaves[index]
        cmd += ['-s', '{}:{}'.format(current_slave, args.p)]
    put_non_required_options(args, cmd)
    return cmd


def get_slave_cmd(args, spec):
    cmd = [sys.executable, os.path.abspath(__file__),
           '-S', spec, '-n', args.n, '-s', args.s, '-o', args.o, '-p', args.p]
    if args.r:
        cmd += ['-r']
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
        raise ValueError('one of -D or -d must be specified')


def get_srun_cmd(args):
    SRUN = ['srun', '-D', '/', '-K', '-q']
    slaves = get_slaves(args)
    spec = ','.join(slaves)
    cmd = SRUN + \
        ['-N', str(len(slaves)), '-w', spec] + \
        get_slave_cmd(args, spec)
    return cmd


def init_process(cmd, read_fd=None, write_fd=None):
    assert not (read_fd and write_fd)
    if read_fd is not None:
        process = subprocess.Popen(cmd, stdin=os.fdopen(read_fd))
    elif write_fd is not None:
        process = subprocess.Popen(cmd, stdout=os.fdopen(write_fd, 'w'))
    else:
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        process.stdin.close()
    return process


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
    procs = {proc.pid: proc for proc in map(init_process, cmds)}
    return wait(procs)


def run_slave(args, env):
    return subprocess.call(get_slave_ndd_cmd(args, env))


def get_host(source):
    return source[source.index('@')+1:] if '@' in source else source


def get_ssh_slave_cmd(args, slave, slaves):
    spec = ','.join(slaves)
    cmd = [sys.executable, os.path.abspath(__file__),
           '-S', spec, '-n', args.n, '-s', args.s,\
           '-o', args.o, '-p', args.p, '-c', slave, '-H']
    if args.r:
        cmd += ['-r']
    put_non_required_options(args, cmd)
    return cmd


def get_ssh_slave_ndd_cmd(args, slave, slaves, write_fd=None):
    cmd = [args.n]
    cmd += get_slave_output_args(args, write_fd)
    index = slaves.index(slave)
    source = args.s if index == 0 else slaves[index-1]
    cmd += ['-r', '{}:{}'.format(get_host(source), args.p)]
    if index != len(slaves) - 1:
        cmd += ['-s', '{}:{}'.format(get_host(slave), args.p)]
    put_non_required_options(args, cmd)
    return cmd


def get_ssh_cmds(args):
    SSH = ['ssh', '-tt', '-o', 'PasswordAuthentication=no']
    slaves = args.d
    cmds = [(SSH + [slave] + get_ssh_slave_cmd(args, slave, slaves))\
            for slave in slaves]
    return cmds


def run_ssh_master(args):
    if args.r:
        read_fd, write_fd = os.pipe()
        tar = init_process(['tar', '-c', args.i], write_fd=write_fd)
    else:
        read_fd = None
    cmds = [get_master_ndd_cmd(args, read_fd)] + get_ssh_cmds(args)
    procs = {proc.pid: proc for proc in map(init_process, cmds)}
    if args.r:
        procs[tar.pid] = tar
    return wait(procs)


def run_ssh_slave(args, env):
    slave = args.c
    slaves = args.S.split(',')
    if args.r:
        read_fd, write_fd = os.pipe()
        tar = init_process(['tar', '-xC', args.o], read_fd=read_fd)
    else:
        write_fd = None
    ndd = init_process(get_ssh_slave_ndd_cmd(args, slave, slaves, write_fd))
    procs = {ndd.pid: ndd}
    if args.r:
        procs[tar.pid] = tar
    return wait(procs)


def main(raw_args, env):
    if len(raw_args) > 0 and raw_args[0] == '-S':
        args = get_slave_parser().parse_args(raw_args)
        if '-H' in raw_args:
            return run_ssh_slave(args, env)
        else:
            return run_slave(args, env)
    elif '-H' in raw_args:
        return run_ssh_master(get_master_parser().parse_args(raw_args))
    else:
        return run_master(get_master_parser().parse_args(raw_args))


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:], os.environ))

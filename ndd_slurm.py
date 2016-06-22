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
    parser.add_argument(
        '-z', action='store_true', help='enable compression with pigz')


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
        cmd = ['-I', '/dev/stdin']
    else:
        cmd = ['-i', args.i]
    return cmd


def get_slave_output_args(args, write_fd):
    if write_fd:
        cmd = ['-O', '/dev/stdout']
    else:
        cmd = ['-o', args.o]
    return cmd


def get_source_for_slave(args, slaves):
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
    return index, source


def get_slave_cmd(args, slave=None, slaves=None, spec=None):
    assert spec or slave and slaves
    if spec is None:
        spec = ','.join(slaves)
    cmd = [sys.executable, os.path.abspath(__file__),
           '-S', spec, '-n', args.n, '-s', args.s,\
           '-o', args.o, '-p', args.p]
    if args.H:
        cmd += ['-c', slave, '-H']
    if args.r:
        cmd += ['-r']
    if args.z:
        cmd += ['-z']
    put_non_required_options(args, cmd)
    return cmd


def get_slave_ndd_cmd(args, write_fd=None):
    cmd = [args.n]
    cmd += get_slave_output_args(args, write_fd)
    slaves = args.S.split(',')
    index, source = get_source_for_slave(args, slaves)
    cmd += ['-r', '{}:{}'.format(source, args.p)]
    if index != len(slaves) - 1:
        current_slave = slaves[index]
        cmd += ['-s', '{}:{}'.format(current_slave, args.p)]
    put_non_required_options(args, cmd)
    return cmd


def get_ssh_slave_ndd_cmd(args, write_fd=None):
    slave = args.c
    slaves = args.S.split(',')
    cmd = [args.n]
    cmd += get_slave_output_args(args, write_fd)
    index = slaves.index(slave)
    source = args.s if index == 0 else slaves[index-1]
    cmd += ['-r', '{}:{}'.format(get_host(source), args.p)]
    if index != len(slaves) - 1:
        cmd += ['-s', '{}:{}'.format(get_host(slave), args.p)]
    put_non_required_options(args, cmd)
    return cmd


def get_master_ndd_cmd(args, read_fd=None):
    cmd = [args.n]
    cmd += get_master_input_args(args, read_fd)
    cmd += ['-s', '{}:{}'.format(args.s, args.p)]
    put_non_required_options(args, cmd)
    return cmd


def get_srun_cmd(args):
    SRUN = ['srun', '-D', '/', '-K', '-q']
    slaves = get_slaves(args)
    spec = ','.join(slaves)
    cmd = SRUN + \
        ['-N', str(len(slaves)), '-w', spec] + \
        get_slave_cmd(args, spec=spec)
    return cmd


def get_ssh_cmds(args):
    SSH = ['ssh', '-tt', '-o', 'PasswordAuthentication=no']
    slaves = args.d
    cmds = [(SSH + [slave] + get_slave_cmd(args, slave=slave, slaves=slaves))\
            for slave in slaves]
    return cmds


def get_host(source):
    return source[source.index('@')+1:] if '@' in source else source


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


def run_slave_tar(args, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['tar', '-xC', args.o], read_fd=read_fd))
    return write_fd


def run_slave_comp(args, procs):
    read_fd, write_fd = os.pipe()
    if args.r:
        tar_w = run_slave_tar(args, procs)
        procs.append(init_process(['pigz', '-d'], read_fd=read_fd,
                                  write_fd=tar_w))
    else:
        out_fd = os.open(args.o, os.O_CREAT | os.O_WRONLY)
        procs.append(init_process(['pigz', '-dc'], read_fd=read_fd,
                                  write_fd=out_fd))
    return write_fd


def run_slave(args):
    procs = []
    if not (args.r or args.z):
        write_fd = None
    if args.z:
        write_fd = run_slave_comp(args, procs)
    elif args.r:
        write_fd = run_slave_tar(args, procs)
    if args.H:
        cmd = get_ssh_slave_ndd_cmd(args, write_fd)
    else:
        cmd = get_slave_ndd_cmd(args, write_fd)
    procs.append(init_process(cmd, write_fd=write_fd))
    return wait(procs)


def run_master_tar(args, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['tar', '-c', args.i],
                              write_fd=write_fd))
    return read_fd


def run_master_comp(args, procs):
    read_fd, write_fd = os.pipe()
    if args.r:
        tar_r = run_master_tar(args, procs)
        procs.append(init_process(['pigz', '--fast'], read_fd=tar_r,
                                  write_fd=write_fd))
    else:
        in_fd = os.open(args.i, os.O_RDONLY)
        procs.append(init_process(['pigz', '--fast', '-c'], read_fd=in_fd,
                                  write_fd=write_fd))
    return read_fd


def run_master(args):
    procs = []
    if not (args.r or args.z):
        read_fd = None
    if args.z:
        read_fd = run_master_comp(args, procs)
    elif args.r:
        read_fd = run_master_tar(args, procs)
    master_cmd = get_master_ndd_cmd(args, read_fd)
    procs.append(init_process(master_cmd, read_fd=read_fd))
    cmds = get_ssh_cmds(args) if args.H else [get_srun_cmd(args)]
    procs.extend(map(init_process, cmds))
    return wait(procs)


def init_process(cmd, read_fd=None, write_fd=None):
    if read_fd and write_fd:
        process = subprocess.Popen(cmd, stdin=os.fdopen(read_fd),
                                   stdout=os.fdopen(write_fd, 'w'),
                                   close_fds=True)
    elif read_fd:
        process = subprocess.Popen(cmd, stdin=os.fdopen(read_fd),
                                   close_fds=True)
    elif write_fd:
        process = subprocess.Popen(cmd, stdout=os.fdopen(write_fd, 'w'),
                                   close_fds=True)
    else:
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        process.stdin.close()
    return process


def wait(procs):
    proc_map = {proc.pid: proc for proc in procs}
    def kill():
        for proc in proc_map.itervalues():
            if proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait()
                except:
                    pass

    for _ in proc_map:
        (pid, status) = os.wait()
        assert pid in proc_map
        proc = proc_map[pid]
        assert proc.wait() is not None
        if status != 0 or proc.returncode != 0:
            kill()
            return 1
    return 0


def main(raw_args):
    if len(raw_args) > 0 and raw_args[0] == '-S':
        return run_slave(get_slave_parser().parse_args(raw_args))
    else:
        return run_master(get_master_parser().parse_args(raw_args))


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

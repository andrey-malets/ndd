#!/usr/bin/env python

import argparse
import os
import socket
import subprocess
import sys
import warnings


def itervalues(d):
    if sys.version_info[0] == 2:
        return dictionary.itervalues()
    return iter(dictionary.values())


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


def get_master_input_args(ndd_input):
    return ['-i', ndd_input] if ndd_input else ['-I', '/dev/stdin']


def get_slave_output_args(ndd_output):
    return ['-o', ndd_output] if ndd_output else ['-O', '/dev/stdout']


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
           '-S', spec, '-n', args.n, '-s', args.s,
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
    cmd += get_slave_output_args(None if write_fd else args.o)
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
    cmd += get_slave_output_args(None if write_fd else args.o)
    index = slaves.index(slave)
    source = args.s if index == 0 else slaves[index-1]
    cmd += ['-r', '{}:{}'.format(get_host(source), args.p)]
    if index != len(slaves) - 1:
        cmd += ['-s', '{}:{}'.format(get_host(slave), args.p)]
    put_non_required_options(args, cmd)
    return cmd


def get_master_ndd_cmd(args, read_fd=None):
    cmd = [args.n]
    cmd += get_master_input_args(None if read_fd else args.i)
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
    cmds = [(SSH + [slave] + get_slave_cmd(args, slave=slave, slaves=slaves))
            for slave in slaves]
    return cmds


def get_host(source):
    return source[source.find('@')+1:]


def get_idle_nodes(partition):
    assert partition
    res = subprocess.check_output(
        ['sinfo', '-p', partition, '-t', 'idle', '-h', '-o' '%n'])
    return res.strip().split(b'\n')


def get_slaves(args):
    if args.D is not None:
        return get_idle_nodes(args.D)
    elif args.d is not None:
        return args.d
    else:
        raise ValueError('one of -D or -d must be specified')


def start_slave_tar(args, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['tar', '-C', args.o, '-x', '-f', '-'],
                              read_fd=read_fd))
    return write_fd


def start_slave_decompressor(output_fd, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['pigz', '-d'], read_fd=read_fd,
                              write_fd=output_fd))
    return write_fd


def start_slave_unpack(args, procs):
    if args.r:
        write_fd = start_slave_tar(args, procs)
    else:
        write_fd = os.open(args.o, os.O_CREAT | os.O_WRONLY)
    if args.z:
        write_fd = start_slave_decompressor(write_fd, procs)
    return write_fd


def run_slave(args):
    procs = []
    if args.r or args.z:
        write_fd = start_slave_unpack(args, procs)
    else:
        write_fd = None
    if args.H:
        cmd = get_ssh_slave_ndd_cmd(args, write_fd)
    else:
        cmd = get_slave_ndd_cmd(args, write_fd)
    procs.append(init_process(cmd, write_fd=write_fd))
    return wait(procs)


def start_master_tar(args, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['tar', '-C', args.i, '-f', '-', '-c', '.'],
                              write_fd=write_fd))
    return read_fd


def start_master_compressor(input_fd, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['pigz', '--fast'], read_fd=input_fd,
                              write_fd=write_fd))
    return read_fd


def start_master_pack(args, procs):
    if args.r:
        read_fd = start_master_tar(args, procs)
    else:
        read_fd = os.open(args.i, os.O_RDONLY)
    if args.z:
        read_fd = start_master_compressor(read_fd, procs)
    return read_fd


def run_master(args):
    procs = []
    if args.r or args.z:
        read_fd = start_master_pack(args, procs)
    else:
        read_fd = None
    master_cmd = get_master_ndd_cmd(args, read_fd)
    procs.append(init_process(master_cmd, read_fd=read_fd))
    slave_control_cmds = get_ssh_cmds(args) if args.H\
                                            else [get_srun_cmd(args)]
    procs.extend(map(init_process, slave_control_cmds))
    return wait(procs)


def init_process(cmd, read_fd=None, write_fd=None):
    if read_fd or write_fd:
        stdin = os.fdopen(read_fd) if read_fd else None
        stdout = os.fdopen(write_fd, 'w') if write_fd else None
        process = subprocess.Popen(cmd, stdin=stdin, stdout=stdout,
                                   close_fds=True)
    else:
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        process.stdin.close()
    return process


def wait(procs):
    proc_map = {proc.pid: proc for proc in procs}
    def kill():
        for proc in itervalues(proc_map):
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
    if raw_args and raw_args[0] == '-S':
        return run_slave(get_slave_parser().parse_args(raw_args))
    else:
        return run_master(get_master_parser().parse_args(raw_args))


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

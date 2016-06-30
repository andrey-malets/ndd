#!/usr/bin/env python

import argparse
import os
import subprocess
import sys


def put_non_required_options(args, cmdline):
    vargs = vars(args)
    for name, sname in [('buffer', 'B'), ('block', 'b')]:
        if vargs.get(name):
            cmdline += ['-{}'.format(sname), str(vargs[name])]


def add_common_options(parser):
    parser.add_argument('-n', '--ndd', metavar='NDD',
                        help='path to alternate ndd binary', default='ndd')
    parser.add_argument('-B', '--buffer',
                        metavar='SIZE', help='buffer size')
    parser.add_argument('-b', '--block',
                        metavar='SIZE', help='block size')
    parser.add_argument('-p', '--port', metavar='PORT', default='3634',
                        help='alternate port for communication (default: 3634)')
    parser.add_argument('-r', '--recursive', action='store_true',
                        help='specify that input is a directory')
    parser.add_argument('-z', '--compress', action='store_true',
                        help='enable compression with pigz')


def add_master_options(parser):
    add_common_options(parser)
    parser.add_argument('-l', '--local', action='store_true',
                        default=False, help='run source locally')
    parser.add_argument('-i', '--input', metavar='INPUT',
                        help='input file on source', required=True)
    parser.add_argument('-o', '--output', metavar='OUTPUT',
                        help='output on destination', required=True)
    parser.add_argument('-s', '--source', metavar='SRC',
                        help='source machine', required=True)
    parser.add_argument('-d', '--destination', metavar='DEST',
                        help='destination machine(s)', action='append',
                        required=True)


def add_slave_options(parser):
    add_common_options(parser)
    parser.add_argument('-i', '--input', metavar='INPUT',
                        help='input file on source')
    parser.add_argument('-o', '--output', metavar='OUTPUT',
                        help='input file on source')
    parser.add_argument('-S', '--send', metavar='ADDR',
                        help='address to listen on')
    parser.add_argument('-R', '--receive', metavar='ADDR',
                        help='address to data receive from')


def get_master_parser():
    parser = argparse.ArgumentParser('run ndd with ssh')
    add_master_options(parser)
    return parser


def get_slave_parser():
    parser = argparse.ArgumentParser('run ndd with ssh (slave mode)')
    add_slave_options(parser)
    return parser


def get_slave_cmd(args, input_=None, output=None, receive=None, send=None):
    cmd = [sys.executable, os.path.abspath(__file__),
           '--slave', '--ndd', args.ndd, '--port', args.port]
    put_non_required_options(args, cmd)
    if args.recursive: cmd += ['--recursive']
    if args.compress: cmd += ['--compress']
    if input_: cmd += ['--input', input_]
    if output: cmd += ['--output', output]
    if receive: cmd += ['--receive', receive]
    if send: cmd += ['--send', send]
    return cmd


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
        for proc in proc_map.values():
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


def start_source_tar(args, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['tar', '-C', args.input, '-f', '-', '-c', '.'],
                              write_fd=write_fd))
    return read_fd


def start_source_compressor(input_fd, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['pigz', '--fast'], read_fd=input_fd,
                              write_fd=write_fd))
    return read_fd


def start_source_pack(args, procs):
    if args.recursive:
        read_fd = start_source_tar(args, procs)
    else:
        read_fd = os.open(args.input, os.O_RDONLY)
    if args.compress:
        read_fd = start_source_compressor(read_fd, procs)
    return read_fd


def get_source_input_args(ndd_input):
    return ['-i', ndd_input] if ndd_input else ['-I', '/dev/stdin']


def get_source_ndd_cmd(args, read_fd=None):
    assert args.send, 'must have destination to send on source'
    cmd = [args.ndd]
    cmd += get_source_input_args(None if read_fd else args.input)
    cmd += ['-s', '{}:{}'.format(args.send, args.port)]
    put_non_required_options(args, cmd)
    return cmd


def run_source(args):
    procs = []
    read_fd = (
        start_source_pack(args, procs)
        if args.recursive or args.compress else None
    )
    procs.append(init_process(
        get_source_ndd_cmd(args, read_fd), read_fd=read_fd))
    return wait(procs)


def start_destination_tar(args, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['tar', '-C', args.output, '-x', '-f', '-'],
                              read_fd=read_fd))
    return write_fd


def start_destination_decompressor(output_fd, procs):
    read_fd, write_fd = os.pipe()
    procs.append(init_process(['pigz', '-d'], read_fd=read_fd,
                              write_fd=output_fd))
    return write_fd


def start_destination_unpack(args, procs):
    if args.recursive:
        write_fd = start_destination_tar(args, procs)
    else:
        write_fd = os.open(args.output, os.O_CREAT | os.O_WRONLY)
    if args.compress:
        write_fd = start_destination_decompressor(write_fd, procs)
    return write_fd


def get_destination_output_args(ndd_output):
    return ['-o', ndd_output] if ndd_output else ['-O', '/dev/stdout']


def get_destination_ndd_cmd(args, write_fd=None):
    assert args.receive, 'must have source to receive on destination'
    cmd = [args.ndd]
    cmd += get_destination_output_args(None if write_fd else args.output)
    cmd += ['-r', '{}:{}'.format(args.receive, args.port)]
    if args.send:
        cmd += ['-s', '{}:{}'.format(args.send, args.port)]
    put_non_required_options(args, cmd)
    return cmd


def run_destination(args):
    procs = []
    write_fd = (
        start_destination_unpack(args, procs)
        if args.recursive or args.compress else None
    )
    procs.append(init_process(get_destination_ndd_cmd(args, write_fd),
                              write_fd=write_fd))
    return wait(procs)


def run_slave(args):
    assert not(args.input and args.output) and \
        (args.input or args.output), \
        'exactly one of input and output is required'
    return run_source(args) if args.input else run_destination(args)


def get_host(source):
    return source[source.find('@')+1:]


def ssh(host):
    return ['ssh', '-tt', '-o', 'PasswordAuthentication=no', host]


def run_master(args):
    procs = []
    source_cmd = get_slave_cmd(
        args, input_=args.input, send=get_host(args.source))
    procs.append(init_process(source_cmd if args.local
                              else ssh(args.source) + source_cmd))
    for i, destination in enumerate(args.destination):
        receive = get_host(args.source if i == 0 else args.destination[i - 1])
        send = (
            get_host(args.destination[i]) if i != len(args.destination) - 1
            else None
        )
        destination_cmd = ssh(destination) + get_slave_cmd(
            args, output=args.output, receive=receive, send=send)
        procs.append(init_process(destination_cmd))
    return wait(procs)


def main(raw_args):
    if len(raw_args) > 0 and raw_args[0] == '--slave':
        return run_slave(get_slave_parser().parse_args(raw_args[1:]))
    else:
        return run_master(get_master_parser().parse_args(raw_args))


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

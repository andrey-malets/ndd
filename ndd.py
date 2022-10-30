#!/usr/bin/env python3

import argparse
import contextlib
import fcntl
import logging
import os
import socket
import subprocess
import sys


def put_non_required_options(args, cmdline):
    vargs = vars(args)
    for name, sname in [('buffer', 'B'), ('block', 'b')]:
        if vargs.get(name):
            cmdline += ['-{}'.format(sname), str(vargs[name])]


def add_common_options(parser):
    parser.add_argument('-li', '--lock-input', metavar='LOCKFILE',
                        help='file to lock on source for reading')
    parser.add_argument('-lo', '--lock-output', metavar='LOCKFILE',
                        help='file to lock on destination(s) for writing')
    parser.add_argument('-n', '--ndd', metavar='NDD',
                        help='path to alternate ndd binary', default='ndd')
    parser.add_argument('-B', '--buffer',
                        metavar='SIZE', help='buffer size')
    parser.add_argument('-b', '--block',
                        metavar='SIZE', help='block size')
    parser.add_argument('-p', '--port', metavar='PORT', default='3634',
                        help='alternate port for communication '
                             '(default: 3634)')
    parser.add_argument('-r', '--recursive', action='store_true',
                        help='specify that input is a directory')
    parser.add_argument('-z', '--compress', action='store_true',
                        help='enable compression with pigz')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose logging')


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
    parser.add_argument('-X', metavar='SSH_ARG', action='append', default=[],
                        help='Additional option(s) to pass to ssh')


def add_slave_options(parser):
    add_common_options(parser)
    parser.add_argument('-i', '--input', metavar='INPUT',
                        help='input file on source')
    parser.add_argument('-o', '--output', metavar='OUTPUT',
                        help='output on destination')
    parser.add_argument('-S', '--send', metavar='ADDR',
                        help='address to listen on')
    parser.add_argument('-R', '--receive', metavar='ADDR',
                        help='address to data receive from')


def get_master_parser():
    parser = argparse.ArgumentParser(description='run ndd with ssh')
    add_master_options(parser)
    return parser


def get_slave_parser():
    parser = argparse.ArgumentParser('run ndd with ssh (slave mode)')
    add_slave_options(parser)
    return parser


def get_local_source_args(args):
    sargs = argparse.Namespace(**vars(args))
    sargs.output = None
    sargs.send = sargs.source
    return sargs


def get_slave_cmd(args, input_=None, output=None, receive=None, send=None):
    cmd = [sys.executable, os.path.abspath(__file__),
           '--slave', '--ndd', args.ndd, '--port', args.port]
    put_non_required_options(args, cmd)

    def add_opt(cmd, name, value, with_value=True):
        if value:
            cmd += ([name, value] if with_value else [name])

    add_opt(cmd, '--verbose', args.verbose, with_value=False)

    add_opt(cmd, '--recursive', args.recursive, with_value=False)
    add_opt(cmd, '--compress', args.compress, with_value=False)

    add_opt(cmd, '--input', input_)
    add_opt(cmd, '--output', output)

    add_opt(cmd, '--receive', receive)
    add_opt(cmd, '--send', send)

    add_opt(cmd, '--lock-input', args.lock_input)
    add_opt(cmd, '--lock-output', args.lock_output)

    return cmd


def get_host(source):
    return source[source.find('@')+1:]


def ssh(host, args):
    return ['ssh', '-tt', '-o', 'PasswordAuthentication=no'] + args + [host]


def get_remote_source_cmd(args):
    return (
        ssh(args.source, args.X) +
        get_slave_cmd(args, input_=args.input, send=get_host(args.source))
    )


def get_remote_destination_cmd(args, i):
    receive = get_host(
        args.source if i == 0 else args.destination[i - 1]
    )
    send = (
        get_host(args.destination[i]) if i != len(args.destination) - 1
        else None
    )
    return (
        ssh(args.destination[i], args.X) +
        get_slave_cmd(args, output=args.output, receive=receive, send=send)
    )


class ProcessWatcher:

    def __init__(self):
        self._processes = set()

    def init_process(self, cmd, read_fd=None, write_fd=None):
        if read_fd or write_fd:
            stdin = os.fdopen(read_fd) if read_fd else None
            stdout = os.fdopen(write_fd, 'w') if write_fd else None
            process = subprocess.Popen(cmd, stdin=stdin, stdout=stdout)
        else:
            process = subprocess.Popen(cmd, stdin=subprocess.DEVNULL)

        self._processes.add(process.pid)
        return process

    def wait(self):
        while self._processes:
            (pid, status) = os.wait()
            logging.debug('Proceess %i exited with status %i', pid, status)
            assert pid in self._processes
            code = os.waitstatus_to_exitcode(status)
            if code < 0:
                raise RuntimeError(
                    f'Process {pid} was terminated by signal {-code}'
                )
            elif code > 0:
                raise RuntimeError(
                    f'Process {pid} exited with non-zero exit code {code}'
                )
            self._processes.remove(pid)


@contextlib.contextmanager
def locked(lockfile, write=False):
    fd = None
    try:
        fd = os.open(lockfile, os.O_RDONLY | os.O_CREAT if write
                     else os.O_RDONLY)
        fcntl.flock(
            fd, (fcntl.LOCK_EX if write else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        yield
    finally:
        if fd:
            os.close(fd)


@contextlib.contextmanager
def locked_on_master(args):
    if args.local and args.lock_input:
        with locked(args.lock_input):
            yield
    else:
        yield


@contextlib.contextmanager
def locked_on_slave(args):
    if args.input and args.lock_input:
        with locked(args.lock_input):
            yield
    elif args.output and args.lock_output:
        with locked(args.lock_output, write=True):
            yield
    else:
        yield


def handle_process(proc, name, rv=None):
    try:
        yield rv
    except BaseException:
        logging.warning('Killing %s', name)
        proc.kill()
        raise
    finally:
        try:
            logging.info('Waiting for %s to stop', name)
            proc.wait()
        except BaseException:
            logging.exception('Failed to cleanup %s', name)


@contextlib.contextmanager
def source_tar(args, watcher):
    read_fd, write_fd = os.pipe()
    cmdline = ['tar', '-C', args.input, '-f', '-', '-c', '.']
    logging.info('Starting source tar: %s', cmdline)
    proc = watcher.init_process(cmdline, write_fd=write_fd)
    yield from handle_process(proc, 'source tar', read_fd)


@contextlib.contextmanager
def source_compressor(args, input_fd, watcher):
    read_fd, write_fd = os.pipe()
    cmdline = ['pigz', '--fast']
    in_fd = (
        os.open(args.input, os.O_RDONLY) if input_fd is None
        else input_fd
    )
    logging.info('Starting source compressor: %s', cmdline)
    proc = watcher.init_process(cmdline, read_fd=in_fd, write_fd=write_fd)
    yield from handle_process(proc, 'source compressor', read_fd)


@contextlib.contextmanager
def prepared_source(args, watcher):
    with contextlib.ExitStack() as stack:
        read_fd = (
            stack.enter_context(source_tar(args, watcher)) if args.recursive
            else None
        )
        if args.compress:
            read_fd = stack.enter_context(
                source_compressor(args, read_fd, watcher)
            )
        yield read_fd


def get_source_ndd_cmd(args, read_fd):
    assert args.send, 'must have destination to send on source'
    cmd = [args.ndd]
    cmd += (
        ['-I', '/dev/stdin'] if read_fd is not None
        else ['-i', args.input]
    )
    cmd += ['-s', '{}:{}'.format(args.send, args.port)]
    put_non_required_options(args, cmd)
    return cmd


@contextlib.contextmanager
def local_source(args, watcher):
    with contextlib.ExitStack() as stack:
        read_fd = stack.enter_context(prepared_source(args, watcher))
        cmd = get_source_ndd_cmd(args, read_fd)
        logging.info('Starting source ndd: %s', cmd)
        proc = watcher.init_process(cmd, read_fd=read_fd)
        yield from handle_process(proc, 'source ndd')


@contextlib.contextmanager
def destination_tar(args, watcher):
    read_fd, write_fd = os.pipe()
    cmdline = ['tar', '-C', args.output, '-x', '-f', '-']
    logging.info('Starting destination tar: %s', cmdline)
    proc = watcher.init_process(cmdline, read_fd=read_fd)
    yield from handle_process(proc, 'destination tar', write_fd)


@contextlib.contextmanager
def destination_decompressor(args, output_fd, watcher):
    read_fd, write_fd = os.pipe()
    cmdline = ['pigz', '-d']
    out_fd = (
        os.open(args.output, os.O_WRONLY | os.O_CREAT) if output_fd is None
        else output_fd
    )
    logging.info('Starting destination decompressor: %s', cmdline)
    proc = watcher.init_process(cmdline, read_fd=read_fd, write_fd=out_fd)
    yield from handle_process(proc, 'destination decompressor', write_fd)


@contextlib.contextmanager
def prepared_destination(args, watcher):
    with contextlib.ExitStack() as stack:
        write_fd = (
            stack.enter_context(destination_tar(args, watcher))
            if args.recursive
            else None
        )
        if args.compress:
            write_fd = stack.enter_context(
                destination_decompressor(args, write_fd, watcher)
            )
        yield write_fd


def get_destination_ndd_cmd(args, write_fd):
    assert args.receive, 'must have source to receive on destination'
    cmd = [args.ndd]
    cmd += (
        ['-O', '/dev/stdout'] if write_fd is not None
        else ['-o', args.output]
    )
    cmd += ['-r', '{}:{}'.format(args.receive, args.port)]
    if args.send:
        cmd += ['-s', '{}:{}'.format(args.send, args.port)]
    put_non_required_options(args, cmd)
    return cmd


@contextlib.contextmanager
def local_destination(args, watcher):
    with contextlib.ExitStack() as stack:
        write_fd = stack.enter_context(prepared_destination(args, watcher))
        cmd = get_destination_ndd_cmd(args, write_fd)
        logging.info('Starting destination ndd: %s', cmd)
        proc = watcher.init_process(cmd, write_fd=write_fd)
        yield from handle_process(proc, 'destination ndd')


@contextlib.contextmanager
def remote_source(args, watcher):
    cmd = get_remote_source_cmd(args)
    logging.info('Running remote source: %s', cmd)
    yield from handle_process(watcher.init_process(cmd), 'remote source')


@contextlib.contextmanager
def remote_destination(args, i, watcher):
    cmd = get_remote_destination_cmd(args, i)
    logging.info('Running remote destination: %s', cmd)
    dhost = get_host(args.destination[i])
    yield from handle_process(watcher.init_process(cmd),
                              f'remote destination {dhost}')


def setup_logging(args):
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format=f'%(asctime)s {socket.gethostname()}: %(levelname)-8s '
               '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main(raw_args):
    slave = (len(raw_args) > 0 and raw_args[0] == '--slave')
    args = (
        get_slave_parser().parse_args(raw_args[1:]) if slave
        else get_master_parser().parse_args(raw_args)
    )

    setup_logging(args)
    watcher = ProcessWatcher()
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            (locked_on_slave if slave else locked_on_master)(args)
        )
        if slave:
            assert (
                args.input and not args.output or
                args.output and not args.input
            ), 'exactly one of input and output is required on slave'

            stack.enter_context(
                local_source(args, watcher) if args.input
                else local_destination(args, watcher)
            )
        else:
            stack.enter_context(
                local_source(get_local_source_args(args), watcher)
                if args.local
                else remote_source(args, watcher)
            )
            for i in range(len(args.destination)):
                stack.enter_context(remote_destination(args, i, watcher))
        try:
            watcher.wait()
        except Exception:
            logging.exception('Failed to wait for child processes')
            return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

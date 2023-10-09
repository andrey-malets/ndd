#!/usr/bin/env python3

import argparse
import contextlib
import dataclasses
import enum
import fcntl
import logging
import os
import signal
import socket
import subprocess
import sys
from typing import Optional


@dataclasses.dataclass
class Process:
    description: str
    cmdline: list[str]
    stdin: Optional[str] = dataclasses.field(default=None)
    stdout: Optional[str] = dataclasses.field(default=None)


@dataclasses.dataclass
class ProcessRunData:
    description: str
    cmdline: str
    stdin_fd: Optional[int] = dataclasses.field(default=None)
    stdout_fd: Optional[int] = dataclasses.field(default=None)
    pass_fds: list[int] = dataclasses.field(default_factory=list)


class PipeType(enum.Enum):
    IN_OUT = 1
    DEV_FD = 2


@dataclasses.dataclass
class Pipe:
    src_id: str
    src_type: PipeType
    dst_id: str
    dst_type: PipeType


@dataclasses.dataclass
class Pipeline:
    processes: dict[str, Process] = dataclasses.field(default_factory=dict)
    pipes: list[Pipe] = dataclasses.field(default_factory=list)


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
    parser.add_argument('-P', '--patch', action='store_true',
                        help='write only blocks that are different in output')
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
    add_opt(cmd, '--patch', args.patch, with_value=False)

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


def prepare_local_source(pipeline, args):
    if args.recursive:
        pipeline.processes['src_tar'] = Process(
            'source tar', ['tar', '-C', args.input, '-f', '-', '-c', '.']
        )
    if args.compress:
        pipeline.processes['src_pigz'] = Process(
            'source compressor',
            (
                ['pigz', '--fast'] if args.recursive else
                ['pigz', '--fast', '--stdout', args.input]
            )
        )

    pipeline.processes['src_socat'] = Process(
        'source socat',
        ['socat', '-u', 'STDIN',
         f'TCP4-LISTEN:{args.port},bind={args.send},reuseaddr'],
        stdin=(
            None if args.recursive or args.compress
            else args.input
        )
    )

    if args.recursive and args.compress:
        pipes = [
            Pipe('src_tar', PipeType.IN_OUT, 'src_pigz', PipeType.IN_OUT),
            Pipe('src_pigz', PipeType.IN_OUT, 'src_socat', PipeType.IN_OUT),
        ]
    elif args.recursive:
        pipes = [
            Pipe('src_tar', PipeType.IN_OUT, 'src_socat', PipeType.IN_OUT),
        ]
    elif args.compress:
        pipes = [
            Pipe('src_pigz', PipeType.IN_OUT, 'src_socat', PipeType.IN_OUT),
        ]
    else:
        pipes = []
    pipeline.pipes.extend(pipes)


def prepare_local_destination(pipeline, args):
    pipeline.processes['dst_rcv_socat'] = Process(
        'destination receiving socat',
        ['socat', '-u', f'TCP4:{args.receive}:{args.port},retry=5', 'STDOUT'],
        stdout=(
            None if (args.recursive or args.compress or args.patch or
                     args.send)
            else args.output
        )
    )
    if args.recursive:
        pipeline.processes['dst_tar'] = Process(
            'destination tar', ['tar', '-C', args.output, '-x', '-f', '-']
        )
    elif args.patch:
        pipeline.processes['dst_bapply'] = Process(
            'destination bapply', ['bapply', args.output]
        )
    if args.compress:
        pipeline.processes['dst_pigz'] = Process(
            'destination decompressor',
            ['pigz', '-d'],
            stdout=(None if (args.recursive or args.patch) else args.output)
        )
    if args.send:
        pipeline.processes['dst_tee'] = Process(
            'destination tee',
            (
                ['tee'] if (args.recursive or args.compress or args.patch)
                else ['tee', args.output]
            )
        )
        pipeline.processes['dst_snd_socat'] = Process(
            'destination sending socat',
            (
                ['socat', '-u', 'STDIN',
                 f'TCP4-LISTEN:{args.port},bind={args.send},reuseaddr']
            )
        )

    pipes = []
    if args.send:
        pipes.extend([
            Pipe('dst_rcv_socat', PipeType.IN_OUT, 'dst_tee', PipeType.IN_OUT),
            Pipe('dst_tee', PipeType.IN_OUT, 'dst_snd_socat', PipeType.IN_OUT),
        ])
        if args.recursive and args.compress:
            pipes.extend([
                Pipe('dst_tee', PipeType.DEV_FD,
                     'dst_pigz', PipeType.IN_OUT),
                Pipe('dst_pigz', PipeType.IN_OUT, 'dst_tar', PipeType.IN_OUT),
            ])
        elif args.recursive:
            pipes.append(
                Pipe('dst_tee', PipeType.DEV_FD, 'dst_tar', PipeType.IN_OUT)
            )
        elif args.compress:
            pipes.append(
                Pipe('dst_tee', PipeType.DEV_FD, 'dst_pigz', PipeType.IN_OUT)
            )
            if args.patch:
                pipes.append(
                    Pipe('dst_pigz', PipeType.IN_OUT,
                         'dst_bapply', PipeType.IN_OUT)
                )
        elif args.patch:
            pipes.append(
                Pipe('dst_tee', PipeType.IN_OUT, 'dst_bapply', PipeType.IN_OUT)
            )
    else:
        if args.recursive and args.compress:
            pipes.extend([
                Pipe('dst_rcv_socat', PipeType.IN_OUT,
                     'dst_pigz', PipeType.IN_OUT),
                Pipe('dst_pigz', PipeType.IN_OUT, 'dst_tar', PipeType.IN_OUT),
            ])
        elif args.recursive:
            pipes.append(
                Pipe('dst_rcv_socat', PipeType.IN_OUT,
                     'dst_tar', PipeType.IN_OUT)
            )
        elif args.compress:
            pipes.append(
                Pipe('dst_rcv_socat', PipeType.IN_OUT,
                     'dst_pigz', PipeType.IN_OUT)
            )
            if args.patch:
                pipes.append(
                    Pipe('dst_pigz', PipeType.IN_OUT,
                         'dst_bapply', PipeType.IN_OUT)
                )
        elif args.patch:
            pipes.append(
                Pipe('dst_rcv_socat', PipeType.IN_OUT,
                     'dst_bapply', PipeType.IN_OUT)
            )

    pipeline.pipes.extend(pipes)


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


def process(run_data, id_):
    assert id_ in run_data, (
        f'No process with id {id_} to connect via pipe'
    )
    return run_data[id_]


def prepare_run_data(processes):
    run_data = {}
    for id_, proc in processes.items():
        stdin_fd = None
        if proc.stdin:
            logging.info('Redirecting stdin for %s from %s',
                         proc.description, proc.stdin)
            stdin_fd = os.open(proc.stdin, os.O_RDONLY)
        stdout_fd = None
        if proc.stdout:
            logging.info('Redirecting stdout for %s to %s',
                         proc.description, proc.stdout)
            stdout_fd = os.open(proc.stdout,
                                os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        run_data[id_] = ProcessRunData(
            description=proc.description,
            cmdline=proc.cmdline,
            stdin_fd=stdin_fd,
            stdout_fd=stdout_fd
        )
    return run_data


def prepare_pipes(run_data, pipes):
    for pipe in pipes:
        logging.info('Using pipe: %s', pipe)
        src_proc = process(run_data, pipe.src_id)
        dst_proc = process(run_data, pipe.dst_id)
        read_fd, write_fd = os.pipe()
        logging.info('Allocated pipe: read_fd=%d, write_fd=%d',
                     read_fd, write_fd)
        if pipe.src_type == PipeType.IN_OUT:
            assert src_proc.stdout_fd is None
            src_proc.stdout_fd = write_fd
        elif pipe.src_type == PipeType.DEV_FD:
            src_proc.cmdline.append(f'/dev/fd/{write_fd}')
            src_proc.pass_fds.append(write_fd)
        else:
            assert False, pipe.src_type

        if pipe.dst_type == PipeType.IN_OUT:
            assert dst_proc.stdin_fd is None
            dst_proc.stdin_fd = read_fd
        elif pipe.dst_type == PipeType.DEV_FD:
            dst_proc.cmdline.append(f'/dev/fd/{read_fd}')
            dst_proc.pass_fds.append(read_fd)
        else:
            assert False, pipe.dst_type


def start_process(rd):
    logging.info('Starting %s: %s, stdin_fd=%s, stdout_fd=%s, pass_fds=%s',
                 rd.description, rd.cmdline, rd.stdin_fd, rd.stdout_fd,
                 rd.pass_fds)
    stdin = (rd.stdin_fd if rd.stdin_fd is not None else subprocess.DEVNULL)
    process = subprocess.Popen(rd.cmdline, stdin=stdin, stdout=rd.stdout_fd,
                               pass_fds=rd.pass_fds)
    if rd.stdin_fd is not None:
        os.close(rd.stdin_fd)
    if rd.stdout_fd is not None:
        os.close(rd.stdout_fd)
    for fd in rd.pass_fds:
        os.close(fd)
    return process


def handle_process_status(description, pid, status):
    logging.debug('%s (pid %d) exited with status %i',
                  description, pid, status)
    code = os.waitstatus_to_exitcode(status)
    if code < 0:
        logging.error('%s (pid %s) was terminated by signal %d (%s)',
                      description, pid, -code, signal.strsignal(-code))
    elif code > 0:
        logging.error('%s (pid %s) exited with non-zero exit code %d',
                      description, pid, code)
    else:
        logging.info('%s (pid %s) exited normally', description, pid)

    return code == 0


def wait_processes(proc_map):
    seen_failures = False
    while not seen_failures and proc_map:
        try:
            (pid, status) = os.wait()
        except ChildProcessError:
            break
        assert pid in proc_map
        seen_failures = (
            seen_failures or
            not handle_process_status(proc_map.pop(pid), pid, status)
        )

    for pid in list(proc_map.keys()):
        description = proc_map.pop(pid)
        (waited_pid, status) = os.waitpid(pid, os.WNOHANG)
        still_running = (waited_pid == 0)
        if seen_failures and still_running:
            logging.warning('Killing %s (pid %s) with SIGTERM',
                            description, pid)
            os.kill(pid, signal.SIGTERM)
            (waited_pid, status) = os.waitpid(pid, 0)
        assert waited_pid == pid
        seen_failures = (
            seen_failures or
            not handle_process_status(description, pid, status)
        )

    return not seen_failures


def execute(pipeline):
    run_data = prepare_run_data(pipeline.processes)
    prepare_pipes(run_data, pipeline.pipes)
    proc_map = {
        start_process(rd).pid: rd.description
        for rd in run_data.values()
    }

    return wait_processes(proc_map)


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
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            (locked_on_slave if slave else locked_on_master)(args)
        )
        pipeline = Pipeline()
        if slave:
            assert (
                args.input and not args.output or
                args.output and not args.input
            ), 'exactly one of input and output is required on slave'

            if args.input:
                prepare_local_source(pipeline, args)
            else:
                prepare_local_destination(pipeline, args)
        else:
            assert not (args.recursive and args.patch), (
                '--recursive and --patch cannot be used at the same time'
            )
            if args.local:
                prepare_local_source(pipeline, get_local_source_args(args))
            else:
                pipeline.processes.append(
                    Process('remote source', get_remote_source_cmd(args))
                )

            for i, dest in enumerate(args.destination):
                pipeline.processes[f'dest_{get_host(dest)}'] = Process(
                    f'remote destination for {get_host(dest)}',
                    get_remote_destination_cmd(args, i)
                )
        try:
            return 0 if execute(pipeline) else 1
        except Exception:
            logging.exception('Failed to wait for child processes')
            return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

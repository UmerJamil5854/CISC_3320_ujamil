#!/usr/bin/env python3
#Run it with:  python3 HW2_ujamil.py

import os
import sys
import shlex
import signal

class Job:
    def __init__(self, pid, command, background):
        self.pid = pid              # also doubles as the job's pgid (see note below)
        self.command = command
        self.background = background
        self.status = "Running"     # Running, Stopped, Done


jobs = {}          # job_id -> Job
next_job_id = 1

def add_job(pid, command, background):
    """Create a Job, store it under a fresh id, and return that id."""
    global next_job_id
    jid = next_job_id
    next_job_id += 1  # ids are never reused, even after a job is removed
    jobs[jid] = Job(pid, command, background)
    return jid


def find_job_by_pid(pid):
    for jid, job in jobs.items():
        if job.pid == pid:
            return jid
    return None


def remove_job(jid):
    jobs.pop(jid, None)


# ---------------------------------------------------------------------------
# Stage 1: Parsing
# ---------------------------------------------------------------------------
class ParsedCommand:
    """Result of parsing one input line."""

    def __init__(self, argv, background, raw):
        self.argv = argv              # tokenized argument list, e.g. ["ls", "-l"]
        self.background = background  # True if line ended with '&'
        self.raw = raw                # original (trimmed) command text, for job display


def parse_line(line):
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    background = False
    if line.endswith("&"):
        background = True
        line = line[:-1].strip()

    try:
        argv = shlex.split(line)
    except ValueError as e:
        print(f"pyshell: parse error: {e}", file=sys.stderr)
        return None

    if not argv:
        return None

    return ParsedCommand(argv, background, line)


BUILTINS = {}


def builtin(name):
    """Decorator to register a function as a built-in command."""
    def wrapper(fn):
        BUILTINS[name] = fn
        return fn
    return wrapper


# ---------------------------------------------------------------------------
# Stage 2: Built-ins (run directly in the shell process, no fork)
# ---------------------------------------------------------------------------
@builtin("cd")
def builtin_cd(argv):
    target = argv[1] if len(argv) > 1 else os.environ.get("HOME", "/")
    try:
        os.chdir(target)
    except OSError as e:
        print(f"cd: {target}: {e.strerror}", file=sys.stderr)


@builtin("exit")
def builtin_exit(argv):
    code = 0
    if len(argv) > 1:
        try:
            code = int(argv[1])
        except ValueError:
            code = 0
    sys.exit(code)


@builtin("jobs")
def builtin_jobs(argv):
    # Refresh statuses first so the listing is current.
    reap_background_jobs(blocking=False)
    for jid in sorted(jobs):
        job = jobs[jid]
        marker = "&" if job.background else ""
        print(f"[{jid}] {job.status:<8} {job.command} {marker}".rstrip())


@builtin("fg")
def builtin_fg(argv):
    jid = _resolve_job_arg(argv)
    if jid is None:
        return
    job = jobs.get(jid)
    if job is None:
        print(f"fg: no such job: {argv[1] if len(argv) > 1 else ''}", file=sys.stderr)
        return

    print(job.command)
    job.background = False
    job.status = "Running"

    # Bring the job's process group into the foreground and continue it.
    try:
        os.tcsetpgrp(sys.stdin.fileno(), job.pid)
    except OSError:
        pass
    try:
        os.killpg(job.pid, signal.SIGCONT)
    except ProcessLookupError:
        pass

    wait_for_job(jid, foreground=True)

    # Restore shell as the foreground process group.
    try:
        os.tcsetpgrp(sys.stdin.fileno(), os.getpgrp())
    except OSError:
        pass


@builtin("bg")
def builtin_bg(argv):
    jid = _resolve_job_arg(argv)
    if jid is None:
        return
    job = jobs.get(jid)
    if job is None:
        print(f"bg: no such job: {argv[1] if len(argv) > 1 else ''}", file=sys.stderr)
        return
    job.background = True
    job.status = "Running"
    try:
        os.killpg(job.pid, signal.SIGCONT)
    except ProcessLookupError:
        pass
    print(f"[{jid}] {job.command} &")


@builtin("kill")
def builtin_kill(argv):
    if len(argv) < 2:
        print("kill: usage: kill [-SIGNAL] %jobid|pid", file=sys.stderr)
        return

    sig = signal.SIGTERM
    target_arg = argv[1]
    if target_arg.startswith("-"):
        try:
            sig = int(target_arg[1:])
        except ValueError:
            sig = getattr(signal, target_arg[1:].upper(), signal.SIGTERM)
        if len(argv) < 3:
            print("kill: missing target", file=sys.stderr)
            return
        target_arg = argv[2]

    if target_arg.startswith("%"):
        jid = int(target_arg[1:])
        job = jobs.get(jid)
        if job is None:
            print(f"kill: no such job: {target_arg}", file=sys.stderr)
            return
        try:
            os.killpg(job.pid, sig)
        except ProcessLookupError:
            pass
    else:
        try:
            os.kill(int(target_arg), sig)
        except (ValueError, ProcessLookupError) as e:
            print(f"kill: {target_arg}: {e}", file=sys.stderr)


def _resolve_job_arg(argv):
    """Interpret fg/bg's optional %N argument; default to most recent job."""
    if len(argv) > 1:
        arg = argv[1]
        if arg.startswith("%"):
            arg = arg[1:]
        try:
            return int(arg)
        except ValueError:
            print(f"usage: {argv[0]} [%jobid]", file=sys.stderr)
            return None
    if not jobs:
        print(f"{argv[0]}: no current job", file=sys.stderr)
        return None
    return max(jobs.keys())


# ---------------------------------------------------------------------------
# Stage 3: External commands — fork() + exec()
# ---------------------------------------------------------------------------
def run_external(parsed):
    """Fork a child and exec the requested program in it."""
    argv = parsed.argv

    try:
        pid = os.fork()
    except OSError as e:
        print(f"pyshell: fork failed: {e}", file=sys.stderr)
        return

    if pid == 0:
        child_pid = os.getpid()
        try:
            os.setpgid(child_pid, child_pid)
        except OSError:
            pass

        if not parsed.background:
            try:
                os.tcsetpgrp(sys.stdin.fileno(), child_pid)
            except OSError:
                pass

        # Children take default signal dispositions (Ctrl-C should kill
        # the child, not the shell).
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTSTP, signal.SIG_DFL)
        signal.signal(signal.SIGTTOU, signal.SIG_DFL)

        try:
            os.execvp(argv[0], argv)
        except FileNotFoundError:
            print(f"pyshell: command not found: {argv[0]}", file=sys.stderr)
            os._exit(127)
        except PermissionError:
            print(f"pyshell: permission denied: {argv[0]}", file=sys.stderr)
            os._exit(126)
        except OSError as e:
            print(f"pyshell: {argv[0]}: {e}", file=sys.stderr)
            os._exit(1)

    else:
        try:
            os.setpgid(pid, pid)
        except OSError:
            pass

        jid = add_job(pid, parsed.raw, parsed.background)

        if parsed.background:
            print(f"[{jid}] {pid}")
        else:
            wait_for_job(jid, foreground=True)
            try:
                os.tcsetpgrp(sys.stdin.fileno(), os.getpgrp())
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Stage 5: Waiting and reaping
# ---------------------------------------------------------------------------
def wait_for_job(jid, foreground):
    """
    Block on a specific job's process until it exits or stops.
    Used for: newly-launched foreground jobs, and `fg` on a background job.
    """
    job = jobs.get(jid)
    if job is None:
        return

    while True:
        try:
            wpid, status = os.waitpid(job.pid, os.WUNTRACED)
        except ChildProcessError:
            remove_job(jid)
            return
        except InterruptedError:
            continue

        if os.WIFSTOPPED(status):
            job.status = "Stopped"
            print(f"\n[{jid}]+ Stopped\t{job.command}")
            return  # control returns to the shell prompt
        elif os.WIFEXITED(status) or os.WIFSIGNALED(status):
            remove_job(jid)
            return


def reap_background_jobs(blocking=False):
    flags = 0 if blocking else os.WNOHANG
    while True:
        try:
            pid, status = os.waitpid(-1, flags | os.WUNTRACED)
        except ChildProcessError:
            break  # no children left at all
        except InterruptedError:
            continue

        if pid == 0:
            break  # WNOHANG: nothing has changed state right now

        jid = find_job_by_pid(pid)
        if jid is None:
            continue

        job = jobs[jid]
        if os.WIFSTOPPED(status):
            job.status = "Stopped"
        elif os.WIFCONTINUED(status):
            job.status = "Running"
        elif os.WIFEXITED(status) or os.WIFSIGNALED(status):
            print(f"\n[{jid}]+ Done\t{job.command}")
            remove_job(jid)


# ---------------------------------------------------------------------------
# Dispatch: built-in vs external method selection
# ---------------------------------------------------------------------------
def execute(parsed):
    cmd_name = parsed.argv[0]
    if cmd_name in BUILTINS:
        BUILTINS[cmd_name](parsed.argv)
    else:
        run_external(parsed)


# ---------------------------------------------------------------------------
# Main REPL loop
# ---------------------------------------------------------------------------
def main():
    # The shell ignores job-control signals itself; children restore defaults.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTSTP, signal.SIG_IGN)
    signal.signal(signal.SIGTTOU, signal.SIG_IGN)
    signal.signal(signal.SIGTTIN, signal.SIG_IGN)

    while True:
        # Stage 5: reap any background jobs that finished since last prompt,
        # without blocking.
        reap_background_jobs(blocking=False)

        try:
            line = input("pyshell> ")
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print()
            continue

        parsed = parse_line(line)
        if parsed is None:
            continue

        execute(parsed)


if __name__ == "__main__":
    main()

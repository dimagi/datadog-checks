# Modified version of https://github.com/DataDog/dd-agent/blob/garner/shell-integration/checks.d/shell.py
# that runs the process with ``shell=True``

from functools import wraps
import logging
import subprocess
import tempfile

from checks import AgentCheck

log = logging.getLogger(__name__)


class ShellCheck(AgentCheck):
    """This check provides metrics from a shell command
    """

    METRIC_NAME_PREFIX = "shell"

    def get_instance_config(self, instance):
        command = instance.get('command', None)
        metric_name = instance.get('metric_name', None)
        metric_type = instance.get('metric_type', 'gauge')
        tags = instance.get('tags', [])

        if command is None:
            raise Exception("A command must be specified in the instance")

        if metric_name is None:
            raise Exception("A metric_name must be specified in the instance")

        if metric_type != "gauge" and metric_type != "rate":
            message = "Unsupported metric_type: {0}".format(metric_type)
            raise Exception(message)

        metric_name = "{0}.{1}".format(self.METRIC_NAME_PREFIX, metric_name)

        config = {
            "command": command,
            "metric_name": metric_name,
            "metric_type": metric_type,
            "tags": tags
        }

        return config

    def check(self, instance):
        config = self.get_instance_config(instance)
        command = config.get("command")
        metric_name = config.get("metric_name")
        metric_type = config.get("metric_type")
        tags = config.get("tags")

        output, _, _ = get_subprocess_output(command, self.log, True)

        try:
            metric_value = float(output)
        except (TypeError, ValueError):
            raise Exception("Command must output a number.")

        if metric_type == "gauge":
            self.gauge(metric_name, metric_value, tags=tags)

        else:
            self.rate(metric_name, metric_value, tags=tags)


class SubprocessOutputEmptyError(Exception):
    pass


def get_subprocess_output(command, log, raise_on_empty_output=True):
    """
    Run the given subprocess command and return its output. Raise an Exception
    if an error occurs.

    Modified version of utils.get_subprocess_output that runs with ``shell=True``
    """

    # Use tempfile, allowing a larger amount of memory. The subprocess.Popen
    # docs warn that the data read is buffered in memory. They suggest not to
    # use subprocess.PIPE if the data size is large or unlimited.
    with tempfile.TemporaryFile() as stdout_f, tempfile.TemporaryFile() as stderr_f:
        proc = subprocess.Popen(command, stdout=stdout_f, stderr=stderr_f, shell=True)
        proc.wait()
        stderr_f.seek(0)
        err = stderr_f.read()
        if err:
            log.debug("Error while running {0} : {1}".format(" ".join(command), err))

        stdout_f.seek(0)
        output = stdout_f.read()

    if not output and raise_on_empty_output:
        raise SubprocessOutputEmptyError("get_subprocess_output expected output but had none.")

    return (output, err, proc.returncode)


def log_subprocess(func):
    """
    Wrapper around subprocess to log.debug commands.
    """
    @wraps(func)
    def wrapper(*params, **kwargs):
        fc = "%s(%s)" % (func.__name__, ', '.join(
            [a.__repr__() for a in params] +
            ["%s = %s" % (a, b) for a, b in kwargs.items()]
        ))
        log.debug("%s called" % fc)
        return func(*params, **kwargs)
    return wrapper

subprocess.Popen = log_subprocess(subprocess.Popen)

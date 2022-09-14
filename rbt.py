#!/usr/bin/env python3

import argparse
import collections
import datetime
import json
import os
import pwd
import re
import smtplib
import socket
import subprocess
import sys
import time
import typing
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header

import jinja2


# backups process

# ConfigOptions defines possible configuration options
ConfigOptions = (
    "name",
    "user",
    "template",
    "target",
    "backups",
    "enabled",
    "files",
    "exclude",
    "fakesuper",
    "chown",
    "mysql",
    "bwlimit",
)

# BackupProperties defines standard set of backup configuration properties
BackupProperties = {opt: None for opt in ConfigOptions}
BackupProperties.update(dict(enabled=True))


def verbose_print(msg: str, *args: list, **kwargs: dict) -> None:
    """Print message if verbose flag has been set"""
    if cmd_args.verbose:
        print(msg, *args, **kwargs)


class FileLock(object):
    """Implement simple file locking"""

    def __init__(self, name: str) -> None:
        """Class constructor"""
        self.name = name
        self.acquired = False

    def __enter__(self) -> "FileLock":
        """Acquire lock"""
        if self.acquired:
            return self
        # make sure process is not already running
        if os.path.exists(self.name):
            try:
                pid = int(open(self.name, "r").read())
                try:
                    os.kill(pid, 0)
                    return self
                except OSError:
                    pass
            except ValueError:
                pass
        # acquire lock
        with open(self.name, "w+") as fh:
            fh.write(str(os.getpid()))
            self.acquired = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release acquired lock"""
        if self.acquired:
            os.unlink(self.name)
            self.acquired = False


class BackupDir(str):
    """Generate full path to backup related resources"""

    @property
    def files(self) -> str:
        """Return full path to files backups directory"""
        return f"{self}/files"

    @property
    def completed(self) -> str:
        """Get full path to completed file name"""
        return f"{self}/completed"


class Backup(collections.namedtuple("Backup", ConfigOptions)):
    """Backup object represents a backup job and its properties"""

    @property
    def latest_dir(self) -> BackupDir:
        """Get the full path to the directory of the latest backup"""
        return BackupDir(f"{self.target}/backup.0")

    @property
    def target_dir(self) -> BackupDir:
        """Get the full path to the directory where next backup will go before rotation"""
        return BackupDir(f"{self.target}/backup.{self.backups}")

    @property
    def username(self) -> str:
        """Returns default username to use in connection"""
        return self.user or pwd.getpwuid(os.getuid()).pw_name

    @property
    def options(self) -> typing.List[str]:
        """Generate list of options used to start rsync sub-process"""
        opts = [
            "/usr/bin/rsync",
            "-aRHS" if len(self.files or []) > 1 else "-aHS",
            "--delete",
            "--stats",
            "--no-h",
        ]
        if self.fakesuper:
            opts.append("--fake-super")
        if self.chown:
            opts.append(f"--chown={self.chown}")
        if self.bwlimit:
            opts.append(f"--bwlimit={self.bwlimit}")
        opts.append(f"--link-dest={self.latest_dir.files}")
        for include in self.files or []:
            if self.name == "localhost":
                opts.append(include)
            else:
                opts.append(f"{self.username}@{self.name}:{include}")
        for exclude in self.exclude or []:
            opts.append(f"--exclude={exclude}")
        opts.append(self.target_dir.files)
        return opts

    def rotate(self) -> None:
        """Rotate backups to move latest backup in backup.0 directory"""
        # move target backup out of the way by renaming to backup.tmp
        temp_dir = f"{self.target}/backup.tmp"
        os.rename(self.target_dir, temp_dir)
        # rotate backups
        for idx in range(self.backups - 1, -1, -1):
            src = f"{self.target}/backup.{idx}"
            dst = f"{self.target}/backup.{idx+1}"
            os.rename(src, dst)
        # make target backup last by renaming to backup.0
        os.rename(temp_dir, self.latest_dir)

    def run(self) -> None:
        """Perform backup with rsync, rotate old backups and save stats"""
        # make sure all backup target directories exist
        for idx in range(self.backups):
            backup_dir = f"{self.target}/backup.{idx}"
            if not os.path.isdir(backup_dir):
                os.makedirs(backup_dir)
        # get start time for later reference
        start = int(time.time())
        # make sure there is a target directory for files backups
        if not os.path.isdir(backup.target_dir.files):
            os.makedirs(backup.target_dir.files)
        verbose_print("Starting command: {0}".format(" ".join(self.options)))
        rsync = subprocess.run(self.options, stdout=subprocess.PIPE)
        if rsync.returncode not in (0, 24):
            print(f"[{self.name}] Return code {rsync.returncode}")
            return
        self.rotate()
        # save statistics from the backup job
        re_size = re.compile("Total file size: (\\d+) bytes")
        for line in rsync.stdout.decode("UTF-8").split("\n"):
            match = re_size.match(line)
            if match:
                size = int(match.group(1))
        with open(self.latest_dir.completed, "w+") as fh:
            data = dict(
                name=self.name,
                timestamp=datetime.datetime.now().isoformat(),
                duration=int(time.time()) - start,
                size=size,
            )
            fh.write(json.dumps(data, separators=",:"))


def load_backups(name: str) -> typing.List[Backup]:
    """Load backup specification from the specified source and return Backup objecs listt"""

    def parse_backups(data: object) -> typing.List[Backup]:
        backups = []
        templates = {}
        # parse templates
        for template in data.get("templates", []):
            templates[template.get("name")] = template
        # parse backups
        for backup_item in data.get("servers", []):
            backup_config = dict(BackupProperties)
            if backup_item.get("template"):
                backup_config.update(templates.get(backup_item.get("template", {})))
            backup_config.update(**backup_item)
            for k, v in backup_config.items():
                if type(v) == str:
                    backup_config[k] = str(v).format(**backup_config)
            backups.append(Backup(**backup_config))
        return backups

    if name.startswith("http"):
        req = urllib.request.urlopen(name)
        data = json.loads(req.read().decode("utf-8"))
    else:
        if name.endswith(".json"):
            with open(name, "r") as fh:
                data = json.loads(fh.read())
        else:
            if name.endswith(".py"):
                name = name.removesuffix(".py")
            data = __import__(name).config
    return parse_backups(data)


# backups stats

BackupStatus = collections.namedtuple(
    "BackupStatus", ("name", "status", "mtime", "duration", "size", "comment")
)

# backup statuses
StatusText = {
    0: "OK",
    1: "ERR",
    2: "UNK",
}
StatusCodes = set()

email_template = jinja2.Template(
    """
{% macro ColorStatus(code) %}
{%- if code == 0 %}<b style="color: green">OK</b>{% endif -%}
{%- if code == 1 %}<b style="color: red">ERR</b>{% endif -%}
{%- if code == 2 %}<b style="color: magenta">UNK</b>{% endif -%}
{% endmacro %}
{%- for section, data in sections %}
<h3 style="text-decoration: underline">category: {{section}}</h3>
<table>
    <thead>
        <tr style="background: #ccc">
            <td width="80px"><b>Status</b></td>
            <td width="200px"><b>Name</b></td>
            <td width="200px"><b>Last</b></td>
            <td width="50px"><b>Duration</b></td>
            <td width="100px"><b>Size</b></td>
        </tr>
    </thead>
    <tbody>
    {%- for name, status, mtime, duration, size, comment in data %}
        <tr style="background: #eee">
            <td style="font-weight: bold">[{{ColorStatus(status)}}]</td>
            <td>{{name}}</td>
            <td>{% if mtime %}{{mtime.strftime('%Y/%b/%d %H:%M:%S')}}{% endif %}</td>
            <td>{{duration}}</td>
            <td>{{size|filesizeformat}}</td>
            {% if comment %}<td>{{comment}}</td>{% endif %}
        </tr>
    {% endfor -%}
    </tbody>
</table>
{% endfor -%}
"""
)

console_template = jinja2.Template(
    """
{%- macro ColorStatus(code) %}
{%- if code == 0 %} [\033[01;32mOK\033[00m] {% endif -%}
{%- if code == 1 %}[\033[01;31mERR\033[00m] {% endif -%}
{%- if code == 2 %}[\033[01;35mUNK\033[00m] {% endif -%}
{% endmacro %}
{%- for section, data in sections %}
\033[01;37mcategory: {{section}}\033[00m
\033[01;33mStatus {{"{:<50}".format("Name")}}{{"{:<22}".format("Last")}}{{"{:<10}".format("Duration")-}}
{{"{:<10}".format("Size")}}\033[00m
{%- for name, status, mtime, duration, size, comment in data %}
 {{ColorStatus(status)-}}
 {{"{:<50}".format(name)-}}
 {% if mtime %}{{"{:<22}".format(mtime.strftime('%Y/%b/%d %H:%M:%S'))}}{% else %}{{"{:<21}".format("n/a")}}{% endif -%}
{{"{:<10}".format(duration)}}{{"{:<10}".format(size|filesizeformat)}}{% if comment %}{{comment}}{% endif %}
{%- endfor %}
{% endfor -%}
"""
)


def read_comment(basedir: str) -> str:
    """Read comment file"""
    name = os.path.join(basedir, ".comment")
    if not os.path.exists(name):
        return None
    with open(name, "r") as fh:
        return fh.read().strip()


def read_completed(path: str, today: object) -> typing.Optional[typing.List]:
    """Read completed file"""
    name = os.path.join(path, "backup.0/completed")
    backup_name = os.path.basename(path)
    if os.path.exists(name) and os.path.isfile(name):
        with open(name, "r") as fh:
            data = json.loads(fh.read())
        mtime = datetime.datetime.fromisoformat(data.get("timestamp"))
        return BackupStatus(
            name=data.get("name", backup_name),
            status=0 if mtime.date() == today else 1,
            mtime=mtime,
            duration=str(datetime.timedelta(seconds=int(data.get("duration")))),
            size=data.get("size", 0),
            comment=read_comment(path),
        )
    return BackupStatus(backup_name, 2, None, "n/a", 0, read_comment(path))


def get_backup_status(args: dict, BaseDirList=[]):
    today = datetime.datetime.now().date()
    result_set = []
    for server in BaseDirList:
        ServerName = server[8:]
        # ignore unnecessary files
        if not args.all:
            if os.path.isfile(os.path.join(server, ".ignore")):
                continue
        # check for status plugin in server backup directory
        plugin = os.path.join(server, "status")
        if os.path.isfile(plugin):
            # status plugins return code 0-3 and output in format:
            # [OK|ERR|UNK]:[iso|epoch]:<mtime>:{comment}
            cmd = subprocess.run(["/bin/sh", plugin], capture_output=True)
            if cmd.returncode in (0, 1, 2):
                stat, fmt, mtime_str, duration, size, comment = (
                    cmd.stdout.decode().strip().split(":")
                )
                if fmt.lower() == "iso":
                    mtime = datetime.datetime.fromisoformat(mtime_str)
                elif fmt.lower() == "epoch":
                    mtime = datetime.datetime.fromtimestamp(int(mtime_str))
                else:
                    # plugin error, display it
                    cmd.returncode = 1
                    mtime = datetime.datetime.now()
                # save server status
                result_set.append(
                    BackupStatus(
                        cmd.returncode, ServerName, mtime, duration, int(size), comment
                    )
                )
                StatusCodes.add(StatusText.get(cmd.returncode))
                continue
        # retrieve backup status
        completed = read_completed(server, today)
        result_set.append(completed)
        StatusCodes.add(StatusText.get(completed.status, "ERROR"))

    # sort by status
    return sorted(result_set, key=lambda item: item.status, reverse=True)


def send_email(Message, Subject, From, To, Server):
    # prepare root message
    msgRoot = MIMEMultipart("related")
    msgRoot["Subject"] = Header(
        "Servers Backups Status [{0}]".format(",".join(StatusCodes)),
        "utf-8",
    ).encode()
    msgRoot["To"] = To

    # prepare HTML message
    msgText = MIMEText(
        Message,
        _charset="UTF-8",
        _subtype="html",
    )

    # attach HTML message
    msgRoot.attach(msgText)

    # send email
    mail = smtplib.SMTP(Server)
    mail.sendmail(From, To, msgRoot.as_string())
    mail.quit()


def get_all_data(args: dict) -> list:
    """Prepare and return server backups data"""
    sections = []
    for section in os.listdir(args.root):
        if not os.path.isdir(f"{args.root}/{section}"):
            continue
        SectionServers = []
        for server in os.listdir(f"/backup/{section}"):
            SectionServers.append(f"/backup/{section}/{server}")
        backup_data = get_backup_status(args, SectionServers)
        if len(backup_data):
            sections.append((section, backup_data))
    return sorted(sections, key=lambda item: item[0])


# main program
if __name__ == "__main__":
    hostname = socket.gethostname()

    # parse command line arguments
    parser = argparse.ArgumentParser(
        description="Perform rsync based incremental backups"
    )
    subparser = parser.add_subparsers()

    backup = subparser.add_parser("backup", help="Perform servers backups")
    backup.add_argument(
        "--prefix",
        type=str,
        default="/etc/rbt",
        help="Configuration directory (default: %(default)s)",
    )
    backup.add_argument(
        "--config",
        type=str,
        action="append",
        required=True,
        help="Specify one or more configuration files",
    )
    backup.add_argument("--server", type=str, help="Backup only specified server")
    backup.add_argument(
        "--verbose", action="store_true", help="Print debug info to stdout"
    )

    status = subparser.add_parser("status", help="Get backups statistics")
    status.add_argument(
        "--smtp",
        type=str,
        default="localhost",
        help="Outgoing SMTP server (default: %(default)s)",
    )
    status.add_argument("--mailto", type=str, help="Destination email address")
    status.add_argument(
        "--subject",
        type=str,
        default="Servers Backups Status",
        help="Email subject (default: %(default)s)",
    )
    status.add_argument(
        "--from",
        type=str,
        default=f"status@{hostname}",
        help="Email sender address (default: %(default)s)",
    )
    status.add_argument(
        "--all",
        action="store_true",
        help="Show all backups in stats",
    )
    status.add_argument(
        "--console",
        action="store_true",
        help="Print statistics on terminal",
    )
    status.add_argument(
        "--root",
        type=str,
        default="/backup",
        help="Backups root path",
    )

    # parse arguments
    cmd_args = parser.parse_args()

    # config is only defined in backup subcommand
    if "config" in cmd_args:
        # add prefix to search path
        if cmd_args.prefix and os.path.isdir(cmd_args.prefix):
            sys.path.append(cmd_args.prefix)
        # walk all configuration files
        for config in cmd_args.config:
            # run backup job
            for backup in filter(lambda b: b.enabled, load_backups(config)):
                if cmd_args.server and cmd_args.server != backup.name:
                    if cmd_args.verbose:
                        verbose_print(f"Skipping {backup.name}")
                    continue
                with FileLock(f"{backup.target}/backup.lock") as lock:
                    if not lock.acquired:
                        verbose_print(f"Unable to acquire lock for {backup.name}")
                        continue
                    verbose_print(f"Starting backup {backup.name}".format(backup.name))
                    backup.run()
    # smtp is only defined in status subcommand
    elif "smtp" in cmd_args:
        if cmd_args.mailto or cmd_args.console:
            backup_data = get_all_data(cmd_args)
        if cmd_args.mailto:
            send_email(
                email_template.render(sections=backup_data),
                "Servers Backups Status",
                "status@backup1.cloxter.net",
                cmd_args.mailto,
                cmd_args.smtp,
            )
        if cmd_args.console:
            print(console_template.render(sections=backup_data))

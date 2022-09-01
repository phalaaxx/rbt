#!/usr/bin/env python3

#
# apt-get install python3-tz python3-yaml
#

import argparse
import collections
import datetime
import os
import pwd
import subprocess
import time
import typing

import pytz
import yaml

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


def lock_file(path: str) -> str:
    """Get full path to lock file name"""
    return f"{path}/backup.lock"


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
        opts = ["/usr/bin/rsync", "-aRH", "--delete", "--stats"]
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
        start = time.time()
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
        with open(self.latest_dir.completed, "w+") as fh:
            data = dict(
                name=self.name,
                timestamp=datetime.datetime.now(pytz.timezone(cmd_args.tz)).isoformat(),
                duration=time.time() - start,
            )
            fh.write(yaml.dump(data, default_flow_style=False))


def load_backups(name: str) -> typing.List[Backup]:
    """Load backup specification from the named file and return Backup object"""
    backups = []
    templates = {}
    with open(name, "r") as fh:
        for items in yaml.load(fh.read(), Loader=yaml.SafeLoader):
            # parse templates
            for template in items.get("templates", []):
                templates[template.get("name")] = template
            # parse backups
            for backup_item in items.get("servers", []):
                backup_config = dict(BackupProperties)
                if backup_item.get("template"):
                    backup_config.update(templates.get(backup_item.get("template", {})))
                backup_config.update(**backup_item)
                for k, v in backup_config.items():
                    if type(v) == str:
                        backup_config[k] = str(v).format(**backup_config)
                backups.append(Backup(**backup_config))
    return backups


# main program
if __name__ == "__main__":
    # parse command line arguments
    parser = argparse.ArgumentParser(
        description="Perform rsync based incremental backups"
    )
    subparser = parser.add_subparsers()
    backup = subparser.add_parser("backup", help="Perform servers backups")
    stats = subparser.add_parser("stats", help="Get backups statistics")

    backup.add_argument(
        "--tz",
        type=str,
        default="UTC",
        help="Current server timezone (default: %(default)s)",
    )
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

    # parse arguments
    cmd_args = parser.parse_args()

    # config is only defined in backup subcommand
    if "config" in cmd_args:
        # walk all configuration files
        for config in cmd_args.config:
            # look for configuration file
            if not config.endswith(".yaml"):
                config = f"{config}.yaml"
            if not os.path.exists(config):
                config = f"{cmd_args.prefix}/{config}".format(cmd_args.prefix, config)
            if not os.path.exists(config):
                print(f"ERROR: Configuration file {config} does not exist.")
                continue
            # run backup job
            for backup in filter(lambda b: b.enabled, load_backups(config)):
                if cmd_args.server and cmd_args.server != backup.name:
                    if cmd_args.verbose:
                        verbose_print(f"Skipping {backup.name}")
                    continue
                with FileLock(lock_file(backup.target)) as lock:
                    if not lock.acquired:
                        verbose_print(f"Unable to acquire lock for {backup.name}")
                        continue
                    verbose_print(f"Starting backup {backup.name}".format(backup.name))
                    backup.run()
    # path is only defined in stats subcommand
    elif "path" in cmd_args:
        pass

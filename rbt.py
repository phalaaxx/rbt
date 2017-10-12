#!/usr/bin/env python3

import argparse
import collections
import datetime
import os
import subprocess
import time
import typing

import pytz
import yaml

# BackupProperties defines standard set of backup configuration properties
BackupProperties = dict(name=None, target=None, backups=28, files=None, exclude=None, fakesuper=False, chown=None)


class Backup(collections.namedtuple('Backup', BackupProperties.keys())):
    """Backup object represents a backup job and its properties"""

    # latest_dir returns full path to last backup directory
    @property
    def latest_dir(self) -> str:
        """Get the full path to the directory of the latest backup"""
        return '{0}/backup.0'.format(self.target)

    # target_dir returns target backup directory for rsync
    @property
    def target_dir(self) -> str:
        """Get the full path to the directory where next backup will go before rotation"""
        return '{0}/backup.{1}'.format(self.target, self.backups)

    @property
    def last_completed(self):
        """Get full name of completed file in last performed backup"""
        return '{0}/completed'.format(self.latest_dir)

    @property
    def options(self) -> typing.List[str]:
        """Generate list of options used to start rsync sub-process"""
        opts = ['/usr/bin/rsync', '-aR', '--delete', '--stats']
        if self.fakesuper:
            opts.append('--fake-super')
        if self.chown:
            opts.append('--chown={0}'.format(self.chown))
        opts.append('--link-dest={0}'.format(self.latest_dir))
        for include in self.files or []:
            opts.append('root@{0}:{1}'.format(self.name, include))
        for exclude in self.exclude or []:
            opts.append('--exclude={0}'.format(exclude))
        opts.append(self.target_dir)
        return opts

    def rotate(self) -> None:
        """Rotate backups to move latest backup in backup.0 directory"""
        # move target backup out of the way by renaming to backup.tmp
        temp_dir = '{0}/backup.tmp'.format(self.target)
        os.rename(self.target_dir, temp_dir)
        # rotate backups
        for idx in range(self.backups - 1, -1, -1):
            src = '{0}/backup.{1}'.format(self.target, idx)
            dst = '{0}/backup.{1}'.format(self.target, idx + 1)
            os.rename(src, dst)
        # make target backup last by renaming to backup.0
        os.rename(temp_dir, self.latest_dir)

    def run(self) -> None:
        """Perform backup with rsync, rotate old backups and save stats"""
        # make sure all backup target directories exist
        for idx in range(self.backups):
            backup_dir = '{0}/backup.{1}'.format(self.target, idx)
            if not os.path.isdir(backup_dir):
                os.makedirs(backup_dir)
        # get start time for later reference
        start = time.time()
        rsync = subprocess.run(self.options, stdout=subprocess.PIPE)
        if rsync.returncode not in (24,):
            rsync.check_returncode()
        self.rotate()
        # save statistics from the backup job
        with open(self.last_completed, 'w+') as fh:
            data = dict(
                name=self.name,
                timestamp=datetime.datetime.now(pytz.timezone(args.tz)).isoformat(),
                duration=time.time() - start,
            )
            fh.write(yaml.dump(data, default_flow_style=False))


def load_backups(name: str) -> typing.List[Backup]:
    """Load backup specification from the named file and return Backup object"""
    backups = []
    with open(name, 'r') as fh:
        for yaml_all in yaml.load(fh.read()):
            for yaml_doc in yaml_all.get('servers', []):
                yaml_data = dict(BackupProperties)
                yaml_data.update(**yaml_doc)
                for k, v in yaml_data.items():
                    if type(v) == str:
                        yaml_data[k] = str(v).format(**yaml_data)
                backups.append(Backup(**yaml_data))
    return backups


# main program
if __name__ == '__main__':
    # parse command line arguments
    parser = argparse.ArgumentParser(description='Perform rsync based incremental backups')
    parser.add_argument('--tz', type=str, default='UTC', help='Current server timezone (default: UTC)')
    parser.add_argument('--conf', type=str, default='/etc/rbt', help='Configuration directory (default: /etc/rbt)')
    parser.add_argument('--servers', type=str, action='append', required=True, help='Provide servers configuration')
    args = parser.parse_args()

    # walk all configuration files
    for config in args.servers:
        # look for configuration file
        if not config.endswith('.yaml'):
            config = '{0}.yaml'.format(config)
        if not os.path.exists(config):
            config = '{0}/{1}'.format(args.conf, config)
        if not os.path.exists(config):
            print('ERROR: Configuration file {0} does not exist.'.format(config))
            continue
        # run backup job
        for backup in load_backups(config):
            backup.run()

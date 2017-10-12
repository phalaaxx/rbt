# rbt

RBT stands for Rsync Backup Tool and is a wrapper around rsync to simplify the process of making incremental backups from remote servers to local filesystems with rsync.

Usage
--

The program accepts command line arguments like a yaml file name (or files names) which describe what remote files should be copied and where on the local filesystems to store backups.
A sample yaml configuration file looks like this - server.hostname.com.yaml:

```yaml
---
- templates:
  - name: default
    backups: 28
    files:
      - /etc

- servers:
  - name: server.hostname.com
    backups: 182
    target: /backup/webservers/{name}
    files:
      - /opt
      - /etc
      - /var
      - /home
      - /root
      - /backup
    exclude:
      - /var/lib/mysql
      - /var/lib/postgresql

  - name: server2.hostname.com
    template: default
    target: /backup/customers/{name}
```

Configuration file names are provided with -f command line option. It is possible to provide more than one file to sequentially run backups on multiple servers:

    rbt.py --servers server1.hostname.com --servers server5.otherhostname.com

Configuration files must be in YAML format and file names must end with .yaml extension, however it is not necessary to specify file extension with --servers option. It will be appended if necessary.
If the provided file name does not exist, RBT will try to also search for it in /etc/rbt directory.

One configuration file may specify more than one server to backup (see example above).

Configuration fields
--

Configuration files are in YAML format and have several fields to configure backup jobs. These are:

* name - this is the name of the server that is to be backed up; it must be resolvable address because this is used to connect to the remote server
* backups - the number of backup copies to keep; last (newest) backup copy is always in a directory $target/backup.0
* target - the root directory for server backups
* files - list of files / directories names to copy from the remote server in backup directories
* exclude - list of files / directories to exclude from backups

Authentication
--

RBT does not provide any means to set remote user, port and authentication method. This configuration should be present in ~/.ssh/config instead.


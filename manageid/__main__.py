from . import *
from datetime import *
from dateutil.relativedelta import *


def main():
    import argparse
    import sys
    import logging
    import logging.handlers
    debug = False
    
    # Initialise the global logger
    log = logging.getLogger('mgid')
    log.setLevel(logging.DEBUG)

    # Log to file
    fh = logging.FileHandler('/var/log/manageid.log')
    fh.setLevel(logging.DEBUG)

    # Determine if stdout is used for debug logging or not - based on whether
    # this is a cronjob or a tty session
    ch = logging.StreamHandler(sys.stdout)
    cron = check_if_cronjob()
    if cron:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.DEBUG)

    # Configure the formatting we want
    form = "%(levelname)s %(asctime)s %(name)s %(funcName)s %(lineno)d %(message)s"
    formatter = logging.Formatter(form)
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    log.addHandler(fh)
    log.addHandler(ch)

    # Silence some annoying loggers
    logging.getLogger('googleapiclient').setLevel(logging.CRITICAL)
    logging.getLogger('oauth2client').setLevel(logging.CRITICAL)

    if debug:
        log.debug("Logger initialised ok")

    parser = argparse.ArgumentParser()
    import inspect
    import os
    binarypath=inspect.stack()[-1][1]
    configpath = os.path.abspath(os.path.join(binarypath,'..','..','etc'))
    parser.add_argument('--configdir', default=configpath)
    parser.add_argument('--cluster', default='m3')
    parser.add_argument('--execute', action='store_true')
    subparser = parser.add_subparsers()
    subp_provision_project = subparser.add_parser('provisionproject')
    subp_provision_project.set_defaults(func=provision_project)
    subp_provision_project.add_argument("-n", "--number", type=int, default=10,
                                        help="The number of projects to take action on")
    subp_provision_project.add_argument("--newscratch", action='store_true',
                                        help="Provision a project scratch space on new lustre storage (adding project and inode quota, with extra steps compared to regular projects)")
    # New shortcut to slurm and symlink provisioning
    subp_user_links = subparser.add_parser('userlinks')
    subp_user_links.set_defaults(func=provision_user_links)
    subp_user_links.add_argument('--username')
    subp_user_links.add_argument('--groupid')
    # New symlink provisioning feature
    subp_provision_symlinks = subparser.add_parser('provisionsymlinks')
    subp_provision_symlinks.set_defaults(func=create_homedir_symlinks)
    subp_provision_symlinks.add_argument('--username')
    subp_provision_symlinks.add_argument('--groupid')
    # New slurm association provisioning feature
    subp_provision_slurm_assocs = subparser.add_parser('provisionslurmassocs')
    subp_provision_slurm_assocs.set_defaults(func=create_slurm_associations)
    subp_provision_slurm_assocs.add_argument('--username')
    subp_provision_slurm_assocs.add_argument('--groupid')
    subp_home = subparser.add_parser('home')
    subp_home.set_defaults(func=homedir_handler)
    subp_home.add_argument("-n", "--number", type=int, default=10,
                            help="The number of users to take action on")
    subp_nfs = subparser.add_parser('nfs')
    subp_nfs.set_defaults(func=nfs_handler)
    subp_nfs.add_argument("-n", "--number", type=int, default=10,
                          help="The number of users to take action on")

    args = parser.parse_args()
    if hasattr(args, 'execute'):
        if args.execute:
            import mysubprocess
            mysubprocess.debug = False
    if hasattr(args, 'func'):
        args.func(args)


if __name__ == '__main__':
    main()

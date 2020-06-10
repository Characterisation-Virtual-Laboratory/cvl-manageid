def get_clients_dict(args, clients={}):
    if "alloc" in clients and clients["alloc"]:
        alloc = True
    else:
        alloc = False
    if "ldap" in clients and clients["ldap"]:
        ldap = True
    else:
        ldap = False
    if "slurm" in clients and clients["slurm"]:
        slurm = True
    else:
        slurm = False
    if "lustre" in clients and clients["lustre"]:
        lustre = True
    else:
        lustre = False
    if "slack" in clients and clients["slack"]:
        slack = True
    else:
        slack = False
    if "reports" in clients and clients["reports"]:
        reports = True
    else:
        reports = False
    if "disk_usage" in clients and clients["disk_usage"]:
        disk_usage = True
    else:
        disk_usage = False
    if "zfs" in clients and clients["zfs"]:
        zfs = True
    else:
        zfs = False
    if "karaage" in clients and clients["karaage"]:
        karaage = True
    else:
        karaage = False
    if "projects" in clients and clients["projects"]:
        projects = True
    else:
        projects = False

    allocs_cl, ldap_cl, slurm_cl, lustre_cl, slack_cl, reports_cl,\
            disk_usage_cl, zfs_cl, karaage_cl, projects_cl = get_clients(args, alloc, ldap,
                                                            slurm, lustre,
                                                            slack, reports,
                                                            disk_usage, zfs,
                                                            karaage, projects)
    return {"alloc": allocs_cl, "ldap": ldap_cl, "slurm": slurm_cl,
            "lustre": lustre_cl, "slack": slack_cl, "reports": reports_cl,
            "disk_usage": disk_usage_cl, "zfs": zfs_cl, "karaage": karaage_cl,
            "projects": projects_cl}


def get_clients(args, alloc=True, ldap=True, slurm=True, lustre=True,
                slack=True, reports=True, disk_usage=True, zfs=True,
                karaage=True, projects=True):
    """Reads config files and creates API clients."""
    import os
    import yaml

    config_dir = args.configdir

    # Naming convention: allocs_cl = allocations client, slurm_cl = slurm client
    # etc
    if alloc:
        from mercallocations.allocations import AllocationsClient
        with open(os.path.join(config_dir, 'allocationsconfig.yml')) as f:
            alloc_config = yaml.full_load(f.read())
        allocs_cl = AllocationsClient(
            alloc_config['sheetid'],
            alloc_config['sheetname'],
            secret_file=os.path.join(config_dir, 'client_secret.json'))
        allocs_cl.authorize()
        allocs_cl.load()
    else:
        allocs_cl = None

    if ldap:
        from mercldap.ldap import Client as LdapClient
        with open(os.path.join(config_dir, 'ldapconfig.yml')) as f:
            ldap_config = yaml.full_load(f.read())
        ldap_cl = LdapClient(**ldap_config)
    else:
        ldap_cl = None
    
    if slurm:
        from mercslurm.slurm import SlurmClient
        with open(os.path.join(config_dir, 'slurmconfig.yml')) as f:
            slurm_config = yaml.full_load(f.read())
        slurm_cl = SlurmClient(**slurm_config)
    else:
        slurm_cl = None
    
    if lustre:
        from merclustre.lfsclient import LfsClient
        with open(os.path.join(config_dir, 'lustreconfig.yml')) as f:
            lustre_config = yaml.full_load(f.read())
        lustre_cl = LfsClient(lustre_config)
    else:
        lustre_cl = None
    
    if slack:
        from mercslack.slack import SlackClient
        with open(os.path.join(config_dir, 'slackconfig.yml')) as f:
            slack_config = yaml.full_load(f.read())
        slack_cl = SlackClient(**slack_config)
    else:
        slack_cl = None

    if reports:
        from mercreports.reports import ReportsClient
        with open(os.path.join(config_dir, 'reportsconfig.yml')) as f:
            reports_config = yaml.full_load(f.read())
        reports_cl = ReportsClient(
            reports_config['sheetid'],
            reports_config['sheetname'],
            secret_file=os.path.join(config_dir, 'client_secret.json'))
        reports_cl.authorize()
    else:
        reports_cl = None

    if disk_usage:
        from mercdiskusage import Client as DiskUsageClient
        with open(os.path.join(config_dir, 'influxdb.yml')) as f:
            influxdb_config = yaml.full_load(f.read())
        disk_usage_cl = DiskUsageClient(**influxdb_config)
    else:
        disk_usage_cl = None

    if zfs:
        from merczfs.zfsclient import ZfsClient
        with open(os.path.join(config_dir, 'zfsconfig.yml')) as f:
            zfs_config = yaml.full_load(f.read())

        zfs_cl = ZfsClient(**zfs_config)
    else:
        zfs_cl = None

    if karaage:
        from merckaraage.karaage import Kquery
        with open(os.path.join(config_dir, 'karaageconfig.yml')) as f:
            karaage_config = yaml.full_load(f.read())
        karaage_cl = Kquery(**karaage_config)
    else:
        karaage_cl = None

    if projects:
        from merchpcuser.hpcuser import HPCProjectClient
        with open(os.path.join(config_dir, 'lustreconfig.yml')) as f:
            projects_config = yaml.full_load(f.read())
        projects_cl = HPCProjectClient(**projects_config)
    else:
        projects_cl = None

    return allocs_cl, ldap_cl, slurm_cl, lustre_cl, slack_cl, reports_cl, \
           disk_usage_cl, zfs_cl, karaage_cl, projects_cl


def provision_project(args):
    """ This function does all of the slurm and lustre
    setup for a project/collaboration/group. There is an implicit assumption
    that the project exists as an ldap group already. Otherwise it is
    likely the lustre operations will fail """

    import os
    import yaml
    import logging

    log = logging.getLogger("mgid.provisionproject")
    log.debug("Processing projects for cluster {}".format(args.cluster.lower()))

    # Fetch the clients for this function
    if "m3" in args.cluster.lower() or "monarch" in args.cluster.lower():
        clients = get_clients_dict(args, clients={"alloc": True, "ldap": True,
                                                  "slurm": True, "lustre": True,
                                                  "slack": True})
        storageClient = clients["lustre"]

    else:
        clients = get_clients_dict(args, clients={"alloc": True, "ldap": True,
                                                  "slurm": True, "slack": True,
                                                  "projects": True})
        storageClient = clients["projects"]


    allocsClient = clients["alloc"]
    ldapClient = clients["ldap"]
    slurmClient = clients["slurm"]
    slackClient = clients["slack"]

    # Some variables to facilitate tracking things
    from collections import defaultdict
    projects_to_explore = set()
    projects_to_ignore = set()
    project_counter = 0

    # Prepare the slack payload
    slackMessages = {'fail': set(), 'ok': set(), 'warning': set()}
    cronemoji = 'file_folder'
    cronname = 'manageid-provision-project'

    # Get a list of all slurm parent projects and the accounts associated with
    # them - we'll check this a few times but doing a bulk call like this
    # reduces the load on the slurm controller(s)
    slurm_assocs = slurmClient.get_all_parent_projects()

    # Our first pass through to determine which projects need to be checked in
    # more detail in later stages
    logStr = "Project {} provisioned? {}"
    projectlist = allocsClient.get_projects()

    # Filter out CVL projects if this is a cvl cluster
    if args.cluster.lower() == 'cvl':
        projectlist = [x for x in projectlist if allocsClient.get_project_parent(x) == 'p004']

    for project in sorted(projectlist):

        # Check the slurm parent
        parent = allocsClient.get_project_parent(project)
        try:
            if slurm_assocs[project] == parent:
                log.debug(logStr.format(project, True))
                projects_to_ignore.add(project)
            else:
                log.info(logStr.format(project, False))
                slackMessages['warning'].add(project)
                projects_to_explore.add(project)
        except KeyError as e:
            log.info(logStr.format(project, False))
            projects_to_explore.add(project)

        # Check the lustre/nfs projects
        for mntpt in storageClient.get_mount_points():
            if not storageClient.exists(mntpt, project):
                projects_to_explore.add(project)
            else:
                projects_to_ignore.add(project)
            
        project_counter += 1

    # Print out some stats about the projects we're working with
    log.debug("There are {} projects in total".format(project_counter))
    log.debug("To explore: {}".format(len(projects_to_explore)))
    log.debug("To ignore: {}".format(len(projects_to_ignore)))

    # If this is an execution run, provision the projects and check whether it's
    # worked or not
    if args.execute:

        # Set the upper limit of the number of projects to process
        if len(projects_to_explore) < args.number:
            upper_limit = len(projects_to_explore)
        else:
            upper_limit = args.number

        # Hacky fix to get allocations config
        with open(os.path.join(args.configdir, 'allocationsconfig.yml')) as f:
            alloc_config = yaml.safe_load(f.read())

        # Handle projects that haven't been provisioned
        log.debug("Processing {} projects".format(upper_limit))
        projs_to_explore_limited = list(projects_to_explore)[:upper_limit]
        for project in projs_to_explore_limited:
            # Check the quota type for this project
            quotatype = 'defaultquota'
            if allocsClient.is_mxproject(project):
                quotatype = 'mxquota'
            if allocsClient.is_cryoproject(project):
                quotatype = 'cryoquota'
            if allocsClient.is_cvlproject(project):
                quotatype = 'cvlquota'

            if "m3" in args.cluster.lower() or "monarch" in args.cluster.lower():
                parent = allocsClient.get_project_parent(project)
            if "cvl" in args.cluster.lower():
                parent = 'p004'
            else:
                parent = 'root'

            # Check the slurm parent
            if not slurmClient.accountexists(project, parent):
                slurmClient.createaccount(project, parent)

            # Check the project storage allocations
            for mntpt in storageClient.get_mount_points():
                if not storageClient.exists(mntpt, project):
                    storageClient.create(mntpt, project, alloc_config[quotatype][mntpt], newfs=args.newscratch)

        # Check that things have actually been updated
        updated_slurm_assocs = slurmClient.get_all_parent_projects()
        for project in projs_to_explore_limited:
            # Check the slurm parents
            parent = allocsClient.get_project_parent(project)
            try:
                if updated_slurm_assocs[project] == parent:
                    slackMessages['ok'].add(project)
                    log.debug('slurmParent for {} correct: true'.format(project))
                else:
                    slackMessages['fail'].add(project)
                    log.debug('slurmParent for {} correct: false'.format(project))
            except KeyError as e:
                slackMessages['fail'].add(project)
                log.debug('slurmParent for {} correct: false')

            # Check the project storage allocations
            for mntpt in storageClient.get_mount_points():
                if not storageClient.exists(mntpt, project):
                    slackMessages['fail'].add(project)
                    log.debug("storageClient.exists for {}: false".format(project))
                else:
                    slackMessages['ok'].add(project)
                    log.debug('storageClient.exists for {}: true'.format(project))

    else:
        cronname += '-dryrun'

    # Construct the final part of the slack payload and send it off
    if slackMessages['fail']:
        slackMessages['failed'] = 'to provision project(s) `{}`'.format(
            "`, `".join(sorted(slackMessages['fail'])))
    else:
        slackMessages['failed'] = ''
    if slackMessages['ok']:
        slackMessages['completed'] = 'provisioning project(s) `{}`'.format(
            "`, `".join(sorted(slackMessages['ok'])))
    else:
        slackMessages['completed'] = ''

    slackClient.send_slack(slackMessages['failed'],
                           slackMessages['completed'],
                           slackMessages['warning'], cronname, cronemoji)
    return


def get_user_account_combo(args):
    """Return a dictionary with project code as key, list of members as value.

    Optional inputs:    args.username -->   return only projects that a user is
                                            a member of.
                        args.groupid  -->   return only the users for project.

    Otherwise return all projects and users.
    """
    import os
    import logging
    from collections import defaultdict

    account_user_dict_ldap = defaultdict(list)

    # Initialise clients
    from merchpcuser.hpcuser import HPCUserClient
    hpcClient = HPCUserClient()

    # Fetch the clients for this function
    clients = get_clients_dict(args, clients={"alloc": True, "ldap": True})
    allocsClient = clients["alloc"]
    ldapClient = clients["ldap"]

    log = logging.getLogger('mgid.get_user_acct')

    if args.username:
        if hpcClient.home_dir_exists(args.username):
            log.info("homedir exists for {}".format(args.username))

            groups = hpcClient.get_user_groups(args.username, args.configdir)
            log.info("{} belongs to groups {}".format(args.username, groups))

            for g in groups:
                g = g[0].decode('utf-8')
                account_user_dict_ldap[g].append(args.username)
        else:
            log.critical("homedir doesn't exist for {}".format(args.username))

    elif args.groupid:
        group_members = ldapClient.getMembers(args.groupid)
        group_members = [m.split(",")[0][4:] for m in group_members]
        log.info("Group {} has members {}".format(args.groupid,
                                                  ", ".join(group_members)))
        account_user_dict_ldap[args.groupid] = group_members

    else:
        # For all projects
        validprojs = [p for p in allocsClient.get_projects()]
        if args.cluster.lower() == 'cvl':
            validprojs = [p for p in validprojs if allocsClient.get_project_parent(p) == 'p004']

        for p in validprojs:
            group_members = ldapClient.getMembers(p)
            group_members = [m.split(",")[0][4:] for m in group_members]
            log.debug(
                "Group {} has members {}".format(p, ", ".join(group_members))
            )
            account_user_dict_ldap[p] = group_members

    return account_user_dict_ldap


def create_slurm_associations(args, account_user_dict_ldap=None):
    """Create the project associations for users"""
    import logging
    log = logging.getLogger('mgid.create_slurm_assocs')
    from collections import defaultdict

    # Fetch the clients for this function
    clients = get_clients_dict(args, clients={"slurm": True, "slack": True})
    slurmClient = clients["slurm"]
    slackClient = clients["slack"]

    slurm_status = slurmClient.check_slurm_status()
    if slurm_status != 0:
        logging.debug('Slurm controller is unavailable, exiting...')
        # import sys
        # sys.exit(1)
        
    # Empty vars for slack
    slackf = ''
    slackg = ''
    slackw = ''
    slackgdict = defaultdict(list)
    slackrdict = defaultdict(list)

    if account_user_dict_ldap is None:
        account_user_dict_ldap = get_user_account_combo(args)

    account_user_dict_slurm = slurmClient.get_cluster_associations(args.cluster.lower())
    slurm_accounts = account_user_dict_slurm.keys()

    for account, group_members in account_user_dict_ldap.items():
        if account in slurmClient.get_mx_caps():
            log.debug('Account is MX CAP, ignoring')
            continue

        log.debug("Checking slurm associations for group {} "
                  "with users {}".format(account, ", ".join(group_members)))

        if account not in slurm_accounts:
            log.critical("Slurm account {} does not exist, has the project been provisioned?".format(account))
            slackf += "Slurm account `{}` does not exist, has the project been provisioned?\n".format(account)
            continue
        
        slurm_users = account_user_dict_slurm[account]

        if len(group_members) == 0:
            log.debug("No group members in {}".format(account))
            slackw += "No group members in {}\n".format(account)
            continue
        elif len(group_members) == 1:
            member = group_members[0]
            if member in slurm_users:
                pass
            else:
                log.critical('Adding user {} to slurm assoc {}'.format(member,
                                                                       account))
                slurmClient.add_user(account, member)
                slackgdict[member].append(account)
        else:
            for member in group_members:
                if member in slurm_users:
                    pass
                else:
                    log.critical(
                        'Adding user {} to slurm assoc {}'.format(member,
                                                                  account)
                    )
                    slurmClient.add_user(account, member)
                    slackgdict[member].append(account)

            ## Commented out by KW to prevent slurmdb crashes due to rm user
            # for member in slurm_users:
            #     if member in group_members:
            #         pass
            #     else:
            #         log.critical(
            #             'Removing user {} from slurm assoc {}'.format(member,
            #                                                           account)
            #         )
            #         slurmClient.remove_user(account, member)
            #         slackrdict[member].append(account)

        log.debug(
            'Checking slurm associations for group {} with '
            'users {} is complete'.format(account, ", ".join(group_members)))

    for user in slackgdict.keys():
        slackg += 'Adding user `{}` to slurm assoc(s) `{}`\n'.format(user, ", ".join(slackgdict[user]))

    if args.execute:
        slackClient.send_slack(slackf, slackg, slackw, 'manageid-slurm',
                               'slurm')
    else:
        slackClient.send_slack(slackf, '', slackg, 'manageid-slurm-dry-run',
                               'slurm', True)

    return


def create_homedir_symlinks(args, account_user_dict_ldap=None):
    """Create the project symlinks in user home directories"""
    import os
    import logging
    log = logging.getLogger('mgid.create_homedir_symlinks')

    # TODO: Check if user is root, if not, exit

    # Initialise clients
    from merchpcuser.hpcuser import HPCUserClient
    hpcClient = HPCUserClient()
    # Fetch the clients for this function
    clients = get_clients_dict(args, clients={"lustre": True, "slack": True})
    lfsClient = clients["lustre"]
    slackClient = clients["slack"]

    locations = lfsClient.get_mount_points()
    # Check that the locations exist
    for loc in locations:
        if os.path.isdir(loc):
            continue
        else:
            log.critical("Possible path error in {}, skipping".format(loc))
            locations.remove(loc)

    if len(loc) == 0:
        log.critical('No paths to link users to, exiting!')
        import sys
        sys.exit(1)

    logstring = "Linking user {} to group {} ... {}"

    if account_user_dict_ldap is None:
        account_user_dict_ldap = get_user_account_combo(args)

    for account, group_members in account_user_dict_ldap.items():
        # Exclude the Cryo-EM LDAP group thingy
        if account == 'cy02':
            continue
        
        try:
            if not lfsClient.exists(locations[0], account):
                log.critical("Lustre allocation for {}/{} does not exist, has the project been provisioned?".format(locations[0], account))
                continue
            
            if not lfsClient.exists(locations[1], account):
                log.critical("Lustre allocation for {}/{} does not exist, has the project been provisioned?".format(locations[1], account))
                continue
        except Exception as e:
            log.exception(e)
        
        log.debug("Checking symlinks for group {} "
                  "with users {}".format(account, ", ".join(group_members)))

        for member in group_members:
            res = hpcClient.check_or_make_symlinks(account, member, locations)
            if res:
                status = "DONE"
            else:
                status = "FAILED"

            log.debug(logstring.format(member, account, status))

    if args.execute:
        slackClient.send_slack(hpcClient.slackf, hpcClient.slackg,
                               hpcClient.slackw, 'manageid-symlink',
                               'linked_paperclips')
    else:
        slackClient.send_slack(hpcClient.slackf, '',
                               hpcClient.slackg + hpcClient.slackw,
                               'manageid-symlink-dry-run',
                               'linked_paperclips', True)

    return


def provision_user_links(args):
    account_user_dict_ldap = get_user_account_combo(args)
    create_homedir_symlinks(args, account_user_dict_ldap)
    create_slurm_associations(args, account_user_dict_ldap)
    return


def check_sudo_permission():
    """Try and rename a file that requires sudo permissions, use this as an
    easy way to check if the user has sudo permissions or not as it'll throw
    an IOError exception"""
    try:
        from shutil import copyfile
        copyfile('/etc/os-release', '/etc/manageid.tempfile')
    except IOError as e:
        import sys
        import logging
        log = logging.getLogger('mgid.sudo_check')
        log.exception(e)
        log.critical("You must run as root for this command to work!")
        sys.exit(1)


def check_if_cronjob():
    """Check if we're running in a terminal or not as a hacky way
    to distinguish between cronjobs and interactive runs
    https://stackoverflow.com/questions/4213091/"""
    import sys
    import os
    if os.isatty(sys.stdin.fileno()):
        # Debug mode.
        return False
    else:
        # Cron mode.
        return True


def homedir_handler(args):
    import logging
    import os
    import pwd
    import grp

    # Set up the logging
    log = logging.getLogger('mgid.home')

    # Check if the user has the appropriate permissions to run the function
    check_sudo_permission()

    # Fetch the clients for this function
    clients = get_clients_dict(args, clients={"ldap": True, "slack": True})
    ldapClient = clients["ldap"]
    slackClient = clients["slack"]

    from merchpcuser.hpcuser import HPCUserClient

    # Determine who 'nobody' is and who 'root' is
    unowned = {}
    nobody = pwd.getpwnam('nobody')
    unowned['nobody'] = {'uid': nobody.pw_uid, 'gid': nobody.pw_gid}
    root = pwd.getpwnam('root')
    unowned['root'] = {'uid': root.pw_uid, 'gid': root.pw_gid}

    # Fetch the cluster groups
    user_uids = []
    ou = 'aclgroups'

    # Cluster specific configs for homedirs
    # TODO: Pull this out into a config file
    cluster = args.cluster.lower()
    if cluster == 'm3' or cluster == 'monarch':
        home = '/home'
    elif cluster == 'm3test':
        home = '/mnt/home-m3'
    elif cluster == 'monarchtest':
        home = '/mnt/home-monarch'
    elif cluster == 'cvl':
        home = '/home'
    else:
        log.error("Cluster configuration for {} not available, exiting...".format(cluster))
        import sys
        sys.exit(1)

    # Fetch the cluster acl members
    cluster_acl_users = [x for x in ldapClient.getMembers(cluster, ou=ou)]
    log.debug("cluster ACL has {} members".format(len(cluster_acl_users)))

    # Fetch the uid, gid, and symlink status of each directory in home
    # and store it in a dictionary for use later on
    homedict = {}
    with os.scandir(home) as it:
        for entry in it:
            if entry.is_dir():
                s = entry.stat()
                homedict[entry.name] = {'uid': s.st_uid, 'gid': s.st_gid,
                                        'symlink': entry.is_symlink()}

    # Iterate over the users in the ACL group and determine if they have a
    # homedir that is owned by them, skipping symlinks to lustre
    users_to_explore = {}
    users_to_ignore = {}
    users_with_key_issues = {}
    for user in cluster_acl_users:
        uid = user.split(",")[0].split("=")[-1]
        path = "/".join((home, uid))

        h = HPCUserClient(uid).UserHome(home, uid)
        keys = h.check_user_keys()
        auth = h.check_user_auth()
        idrsa = h.check_user_idrsa()
        if not keys or not auth or not idrsa:
            users_with_key_issues[uid] = ['Keys', keys, 'Auth', auth, 'id_rsa', idrsa]

        # Temporary hack to get around the lustre homedirs
        try:
            if homedict[uid]['symlink']:
                log.debug("Skipping {} as it is a symlink, probably to lustre".format(path))
                continue
        except KeyError as e:
            log.error("ZFS or NFS directory not found for user {}, skipping...".format(uid))
            continue
        except Exception as e:
            log.exception(e)

        # Get the uidnumber, gidnumber, check that the directory has the
        # correct ownership and group membership
        try:
            if h.check_ownership(h.homepath):
                continue
            else:
                users_to_explore[uid] = homedict[uid]
        except Exception as e:
            log.exception(e)
            log.exception("User {} should have a home dir but doesn't, check "
                          "the output of manageid zfs (M3) or manageid nfs "
                          "(MonARCH)")

    log.debug("Users to explore: {}".format(len(users_to_explore)))
    log.debug("Users to ignore: {}".format(len(users_to_ignore)))

    # if len(users_with_key_issues) != 0:
    #     log.info("Users with key issues: {}".format(users_with_key_issues))

    # Set the upper limit of the number of users to process
    if len(users_to_explore) < args.number:
        upper_limit = len(users_to_explore)
    else:
        upper_limit = args.number

    failed = []
    completed = []
    # Handle users who don't have the correct permissions
    log.debug("Processing {} users".format(upper_limit))
    for user in {k: users_to_explore[k] for k in list(users_to_explore)[:upper_limit]}:
        if (users_to_explore[user]['uid'] == unowned['nobody']['uid'] and
                users_to_explore[user]['gid'] == unowned['nobody']['gid']):
            log.debug(
                'Dir /home/{} belongs to nobody:nobody, homedir has likely not been provisioned...'.format(user))
        if users_to_explore[user]['uid'] == unowned['root']['uid'] and \
                users_to_explore[user]['gid'] == unowned['root']['gid']:
            log.debug(
                'Dir /home/{} belongs to root:root, homedir has likely not been provisioned...'.format(user))
        try:
            h = HPCUserClient(user).UserHome(home, user)
            log.info('Homedir exists for user {}: {}'.format(
                user, h.check_user_home())
            )
            log.info('Keys set up for user {}: {}'.format(
                user, h.check_user_keys())
            )
            user_attrs = pwd.getpwnam(user)
            h.fix_home(user_attrs.pw_uid, user_attrs.pw_gid)
            if h.check_user_home() and h.check_ownership(h.homepath) and \
                h.check_user_keys():
                completed.append('`{}`'.format(user))
            else:
                failed.append('`{}`'.format(user))
        except Exception as e:
            log.exception(e)

    # Report on the results
    slack_name = 'manageid-userhome'
    if not args.execute:
        slack_name += '-dryrun'

    if len(failed) != 0:
        fail = 'To copy skeleton and set up keys for {}'.format(
            ', '.join(sorted(failed))
        )
    else:
        fail = ''

    if len(completed) != 0:
        comp = 'Copying skeleton and setting up keys for {}'.format(
            ', '.join(sorted(completed))
        )
    else:
        comp = ''

    slackClient.send_slack(fail, comp, '', slack_name, 'house_with_garden')

    return


def nfs_handler(args):
    import logging
    log = logging.getLogger('mgid.nfs')

    # Check that we're operating on NFS host only, not the login nodes!!
    import os
    import yaml
    with open(os.path.join(args.configdir, 'nfsconfig.yml'), 'r') as f:
        nfsconfig = yaml.full_load(f.read())

    if os.uname().nodename not in nfsconfig['nfshosts']:
        log.critical(
            'This function should only run on {}, not {}! '
            'This function is now exiting...'.format(" or ".join(nfsconfig['nfshosts']), os.uname().nodename))
        import sys
        sys.exit(1)

    # Check that we're running as root
    check_sudo_permission()

    # We've passed the checks so begin checking the state of homedirs
    clients = get_clients_dict(args, clients={"slack": True, "ldap": True})
    slackClient = clients["slack"]
    ldapClient = clients["ldap"]

    from merchpcuser.hpcuser import HPCUserClient

    user_uids = []
    ou = 'aclgroups'
    cluster = args.cluster.lower()

    if cluster != nfsconfig['cluster']:
        log.critical("This function should only be run for {}, you have "
                      "selected the cluster {}".format(nfsconfig['cluster'], cluster))
        import sys
        sys.exit(1)

    # CVL @ UWA hack
    if args.cluster.lower() == 'cvluwa':
        cluster = 'm3'
        log.debug('Cluster is {}, args.cluster is {}'.format(cluster, args.cluster))

    # Fetch the cluster acl members
    cluster_acl_users = [x for x in ldapClient.getMembers(cluster, ou=ou)]
    log.debug("There are {} users in cn={},ou={}".format(len(cluster_acl_users),
                                                         cluster, ou))
    log.debug("This may take a while...")

    # Empty data structures for use later on
    users_to_explore = []
    users_to_ignore = []

    # This block always runs; determines which users are missing home dirs
    for user in cluster_acl_users:
        uid = user.strip().split(",")[0].split("=")[-1]
        h = HPCUserClient().UserHome(nfsconfig['mnt'], uid)
        if not h.check_user_home():
            users_to_explore.append(uid)
        else:
            users_to_ignore.append(uid)

    log.debug("Users to explore: {}".format(len(users_to_explore)))
    log.debug("Users to ignore: {}".format(len(users_to_ignore)))

    # Set the upper limit of the number of users to process
    if len(users_to_explore) < args.number:
        upper_limit = len(users_to_explore)
    else:
        upper_limit = args.number

    # This block runs if args.execute is true
    if args.execute and users_to_explore:
        log.debug("Processing {} users".format(upper_limit))
        for uid in users_to_explore[:upper_limit]:
            try:
                h = HPCUserClient().UserHome(nfsconfig['mnt'], uid)
                if not h.check_user_home():
                    log.debug("Creating homedir for user {}".format(uid))
                    h.create_user_homedir()

                h.mark_folder()
                if args.cluster.lower() == 'monarch':
                    h.set_nfs_quota(nfsconfig['quotas']['user'])
            except Exception as e:
                log.exception(e)
                pass

    # Check that the nfs operations have actually worked
    completed = []
    failed = []
    for uid in users_to_explore[:upper_limit]:
        h = HPCUserClient().UserHome(nfsconfig['mnt'], uid)
        if h.check_user_home():
            completed.append('`{}`'.format(uid))
        else:
            failed.append('`{}`'.format(uid))

        # TODO: Check that quota has been set properly!

    # Report on the results
    slack_name = 'manageid-nfs'
    if not args.execute:
        slack_name += '-dryrun'

    if len(failed) != 0:
        fail = 'Creating nfs dir(s) for {}'.format(', '.join(failed))
    else:
        fail = ''

    if len(completed) != 0:
        comp = 'Creating nfs dir(s) for {}'.format(', '.join(completed))
    else:
        comp = ''

    slackClient.send_slack(fail, comp,
                           'Something went wrong, check the logs',
                           slack_name, 'file_cabinet')

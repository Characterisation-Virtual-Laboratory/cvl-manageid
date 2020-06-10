"""Single class module implementing methods to interact with slurm"""
import mysubprocess as subprocess
import logging

l = logging.getLogger('mgid.slurm')


class SlurmClient(object):
    """Implements all methods for interacting with slurm, creating
    accounts, adding and removing users from those accounts"""
    def __init__(self, slurm_base):
        import os
        self.slurm_base = slurm_base
        self.sacctmgr = os.path.join(self.slurm_base, 'sacctmgr')
        self.sacct = os.path.join(self.slurm_base, 'sacct')
        self.sshare = os.path.join(self.slurm_base, 'sshare')
        self.scontrol = os.path.join(self.slurm_base, 'scontrol')
        self.log = logging.getLogger('mgid.slurm')

    def check_slurm_status(self):
        cmd = [self.scontrol, 'ping']
        scontrol = subprocess.Popen(cmd, query=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        (stdout, __) = scontrol.communicate()
        if 'DOWN/DOWN' in stdout.decode():
            code = 0
            l.critical('Slurm primary and backup controllers are down: {}'.format(stdout.decode()))
        else:
            if 'UP/DOWN' in stdout.decode():
                msg = 'Slurm backup controller is down: {}'
                code = 0
            else:
                msg = 'Slurm controllers are ok: {}'
                code = 1
            logging.debug(msg.format(stdout.decode()))
        return code

    def check_sacctmgr_status(self):
        cmd = [self.sacctmgr, 'show', 'cluster', '-P', '-n']
        sacctmgr = subprocess.Popen(cmd, query=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        (stdout, stderr) = sacctmgr.communicate()

        if "slurm_persist_conn_open_without_init" in stdout.decode() or\
                        "slurm_persist_conn_open_without_init" in stderr.decode():
            l.critical("sacctmgr can't communicate with slurmdbd, exiting...")
            code = 1
        else:
            code = 0
            
        return code

    def all_jobs(self,starttime,endtime):
        import subprocess
        fields = ['JobID','User','Account','AllocCPUS','AllocGRES','ReqGRES','CPUTimeRAW','JobName','Submit','Start','End','State']
        result = []
        cmd  = [self.sacct,'-X','-S',"{}".format(starttime),'-E',"{}".format(endtime),'-a','--format={}'.format(','.join(fields)),'-n','-p']
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (stdout,stderr) = p.communicate()
        if len(stderr) > 0:
            print(stderr)
        for l in stdout.decode().splitlines():
            values = l.split('|')
            job = dict(zip(fields,values))
            result.append(job)
        return result

    def check_account_status(self, account, parent=None):
        """Check that a slurm account exists, ignoring the parent account"""
        cmd = [self.sacctmgr, 'show', 'account', account, 'withassoc',
               'format=parentname', '--parsable', '--noheader']
        sacctmgr = subprocess.Popen(cmd, query=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        (stdout, _) = sacctmgr.communicate()
        if len(stdout.splitlines()) == 0:
            return False
        else:
            return True

    def accountexists(self, account, parent=None):
        """Create the account for the project assigning it to the correct
        parent"""
        
        cmd = [self.sacctmgr, 'show', 'account', account, 'withassoc',
               'format=parentname', '--parsable', '--noheader']
        sacctmgr = subprocess.Popen(cmd, query=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        (stdout, _) = sacctmgr.communicate()
        for assoc in stdout.splitlines():
            fields = assoc.split(b'|')
            if fields[0].decode() == parent:
                self.log.debug("Parent account {} for {} is correct".format(parent,
                                                                     account))
                return True

        self.log.debug("Parent account for {} is not correct".format(account))
        return False

    def createaccount(self, account, parent):
        """Create the account for the project assigning it to the correct
        parent"""
        if parent is None or parent == "":
            return
        if account is None or account == "":
            return
        cmd = [self.sacctmgr, '-i', 'add', 'account', account,
               'parent={}'.format(parent),
               'Organization=Monash', 'set', 'fairshare=1']
        subprocess.call(cmd)
        self.log.info("Assigning {} as parent account of {}".format(parent,
                                                             account))

    def getusers(self, account):
        """Get the list of users currently able to use the account"""

        self.log.debug("Retrieving list of users under {} account".format(account))

        user_set = set()
        cmd = [self.sacctmgr, 'show', 'associations', '--parsable2',
               '--noheader',
               'account={}'.format(account), 'format=user']
        sacctmgr = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, query=True)

        (stdout, _) = sacctmgr.communicate()
        for user in stdout.splitlines():
            user = user.decode('utf-8')
            if user is not None and user is not "":
                user_set.add(user)

        self.log.debug("List of users under {} account: {}".format(account,
                                                            user_set))
        return user_set

    def get_cluster_associations(self, cluster):
        """Get all user/account associations for `cluster` and return them"""

        self.log.debug("Getting {} user & account associations".format(cluster))

        from collections import defaultdict
        associations = defaultdict(list)

        cmd = [self.sacctmgr, 'show', 'associations', 'where',
               'cluster={}'.format(cluster), 'format=account,user',
               '--noheader', '--parsable2']
        sacctmgr = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, query=True)
        (stdout, _) = sacctmgr.communicate()

        # Parse associations into a dictionary
        for res in stdout.splitlines():
            res = res.decode('utf-8').split("|")
            if res[1] is not '':
                account, user = res
                associations[account].append(user)
            else:
                account = res[0]
                associations[account].append('')
        return associations

    def get_account_share_quota_usage(self,account):
        cmd = [self.sshare, '-l', '-A',account, '--format=Account,NormShares,NormUsage', '-P']
        import subprocess
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout,stderr = p.communicate()
        lines = stdout.splitlines()
        if len(lines) == 1:
            return(None,None)
        values = lines[-1].split(b'|')
        try:
            rv = (float(values[1]),float(values[2]))
        except ValueError:
            rv = (None,None)
        return rv

    def get_all_shares(self):
        fields = ['Account','NormShares','NormUsage','LevelFS','RawUsage']
        cmd = [self.sshare, '-l', '--format={}'.format(','.join(fields)), '-P']
        import subprocess
        rv = {}
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout,stderr = p.communicate()
        for l in stdout.splitlines():
            values = l.split(b'|')
            d = dict(zip(fields,values))
            rv.update({d['Account']:d})
        return rv

    def get_shares(self, user):
        """Get the list of accounts and norm shares and usage"""
        fields = ['Account','NormShares','NormUsage']
        cmd = [self.sshare, '-l', '--format={}'.format(','.join(fields)), '-P']
        import subprocess
        rv = {}
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout,stderr = p.communicate()
        for l in stdout.splitlines():
            values = l.split(b'|')
            d = dict(zip(fields,values))
            rv.update({d['Account']:d})
        return rv

    def add_user(self, account, user, cluster=None):
        """Add a user to a slurm account, optionally on a given cluster"""
        self.log.info("Adding user {} to SLURM account {}".format(user, account))

        if cluster is not None:
            cmd = [self.sacctmgr, '-i', 'add', 'user', 'name={}'.format(user),
                   'account={}'.format(account), 'cluster={}'.format(cluster)]
        else:
            cmd = [self.sacctmgr, '-i', 'add', 'user', 'name={}'.format(user),
                   'account={}'.format(account)]
        subprocess.call(cmd)
        import time
        self.log.info("Sleeping for 5 seconds to reduce load on slurmdb")
        time.sleep(5)

    def remove_user(self, account, user, cluster=None):
        """remove a user from a slurm account, optionally on a given cluster"""
        self.log.info("Removing user {} from SLURM account {}".format(user, account))

        if cluster is not None:
            cmd = [self.sacctmgr, '-i', 'delete', 'user',
                   'name={}'.format(user), 'account={}'.format(account),
                   'cluster={}'.format(cluster)]
        else:
            cmd = [self.sacctmgr, '-i', 'delete', 'user',
                   'name={}'.format(user), 'account={}'.format(account)]
        subprocess.call(cmd)
        import time
        self.log.info("Sleeping for 5 seconds to reduce load on slurmdb")
        time.sleep(5)

    @staticmethod
    def get_desired_users(account):
        """Query the OS via getent to determine which users should be added to
        which accounts (each slurm account also has a linux group)"""
        l.debug("Getting list of desired users for account {}".format(account))

        cmd = ['getent', 'group', account]
        getent = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, query=True)
        (stdout, _) = getent.communicate()

        users = stdout.decode('utf-8').strip().split(':')[3]
        user_set = set()
        for user in users.split(','):
            user_set.add(user)

        l.debug("List of desired users for account {}: {}".format(account,
                                                                  user_set))
        return user_set

    def setusers(self, account, cluster=None):
        """Calculates the changes needed for the account and makes them"""
        current_slurm_users = self.getusers(account)
        if account in self.get_mx_caps():
            desired_slurm_users = set()
        else:
            desired_slurm_users = self.get_desired_users(account)
        
        for user in current_slurm_users - desired_slurm_users:
            self.remove_user(account, user, cluster)
        for user in desired_slurm_users - current_slurm_users:
            self.add_user(account, user, cluster)

    def get_project_parent(self,account):
        import mysubprocess as subprocess
        cmd = [self.sacctmgr, 'show', 'Account', account , 'withassoc', '-p']
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout,stderr) = p.communicate()
        lines = stdout.decode().splitlines()
        parentcol = lines[0].split('|').index('Par Name')
        for l in lines[1:]:
            parent = l.split('|')[parentcol] 
            if parent is not None:
                return parent
        return None

    def get_all_parent_projects(self):
        import mysubprocess as subprocess
        cmd = [self.sacctmgr, 'show', 'assoc', 'format=parentname,account', '-P',
               '-n']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, query=True)
        (stdout, stderr) = p.communicate()
        lines = stdout.decode('utf-8').splitlines()

        parent_dict = {}
        for line in lines:
            parent, account = line.split("|")
            # A nice hack to remove duplicates from the list
            if parent != '':
                parent_dict[account] = parent
        return parent_dict

    def get_mx_caps(self):
        return ['ny79', 'sf32', 'va91', 'od25', 'qc45', 'be32']

    def get_user_account_shares(self, user):
        """Get the list of accounts & fairshare value associated with a user"""
        fair_share = {}
        cmd = [self.sshare, '-U', '-u', '{}'.format(user),
               '--format=Account,User,Fairshare', '--noheader', '--parsable2']
        getent = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, query=True)
        (stdout, _) = getent.communicate()

        for assoc in stdout.decode().splitlines():
            if user in assoc:
                fields = assoc.split('|')
                if fields[1].strip() == user and fields[0].strip() != "default":
                    if fields[0].strip() in self.get_mx_caps():
                        fair_share[fields[0]] = float(-1)
                    else:    
                        fair_share[fields[0]] = fields[2]

        if fair_share is not None and len(fair_share) > 0:
            self.log.debug("Found fairshare value(s) for %s", user)
            return fair_share
        else:
            self.log.debug("Fairshare value not found for %s", user)
            return None

    def get_sorted_user_accounts(self, user):
        """Return a list of accounts for a user, sorted from highest to lowest
        priority according to fairshare value"""
        fair_share = self.get_user_account_shares(user)
        try:
            member_of = list(fair_share.keys())
            
            if fair_share is not None and member_of is not None and len(member_of) > 0:
                sorted_fair_share = sorted(member_of,
                                           key=lambda x: fair_share.get(x),
                                           reverse=True)
                self.log.debug("Sorted fair share {}".format(sorted_fair_share))
                return sorted_fair_share
            else:
                return None
        except AttributeError:
            self.log.debug("Account not found for %s", user)

    def get_current_default_account(self, user):
        cmd = [self.sacctmgr, 'show', 'user', 'Format=User,DefaultAccount',
               '--noheader', '--parsable2', 'where', 'user={}'.format(user)]
        getent = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, query=True)
        (stdout, _) = getent.communicate()

        try:
            fields = stdout.decode().split('|')
            current_default_account = fields[1]
            return current_default_account
        except IndexError:
            self.log.debug("Default account not found for %s", user)

    def set_default_account(self, user, account):
        """Set the default account for a user"""
        self.log.info("Setting default account {} for user {}".format(account,
                                                                     user))
        cmd = [self.sacctmgr, '-i', 'modify', 'user', 'where',
               'name={}'.format(user), 'set',
               'DefaultAccount={}'.format(account)]
        subprocess.call(cmd)

    def set_default_account_handler(self, user):
        current_default_account = self.get_current_default_account(user).strip()
        desired_default_account = self.get_sorted_user_accounts(user)[0].strip()
        if desired_default_account in self.get_mx_caps():
            desired_default_account = self.get_sorted_user_accounts(user)[1].strip()

        if current_default_account != desired_default_account:
            self.log.info("Default account is not correct for {} - "
                   "current default account is {} - suggest "
                   "default account is {}""".format(user,
                                                    current_default_account,
                                                    desired_default_account))
            self.set_default_account(user, desired_default_account)

        else:
            self.log.info("Default account is correct for {} - "
                   "default account is {}".format(user,
                                                  current_default_account))

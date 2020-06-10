"""Single class module implementing methods to interact with hpc users,
including homedirs, symlinks, and slurm associations (through mercslurm)"""
import mysubprocess
import os
import logging
import pwd


# TODO: Complete refactor, reduce duplication, split clients properly


class HPCProjectClient(object):
    def __init__(self, mntpts, allocsdata=None):
        self.mntpts = mntpts
        self.allocsdata = allocsdata

    @staticmethod
    def create(mntpt, groupname, quota=None, delay=1):
        """create a directory rooted at /fs, assign ownership and set quotas"""
        import os
        import mysubprocess as subprocess
        import time

        path = os.path.join(mntpt, groupname)
        mkdir = ['/bin/mkdir', path]
        chgrp = ['/bin/chgrp', groupname, path]
        chmod = ['/bin/chmod', '2770', path]

        subprocess.call(mkdir)
        time.sleep(delay)
        subprocess.call(chgrp)
        time.sleep(delay)
        subprocess.call(chmod)


    @staticmethod
    def exists(mntpt, groupname):
        import os
        return os.path.isdir(os.path.join(mntpt, groupname))

    def get_mount_points(self):
        return self.mntpts


class HPCUserClient(object):
    """Implements all methods for interacting with user homedirs, creating
    homedirs, ssh keys, and project symlinks"""

    def __init__(self, mnt='/home'):
        self.sacctmgr = 'sacctmgr'
        self.slackw = ''
        self.slackg = ''
        self.slackf = ''
        self.skel = '/usr/local/hpcusr/latest/skel'
        self.mnt = mnt

    def home_dir_exists(self, user):
        """Check that mnt/user exists"""
        import os
        return os.path.exists(os.path.join(self.mnt, user))

    def get_file_ownership(self, filename):
        import os
        from pwd import getpwuid
        from grp import getgrgid
        return (
            getpwuid(os.lstat(filename).st_uid).pw_name,
            getgrgid(os.lstat(filename).st_gid).gr_name
        )

    def check_or_make_symlinks(self, proj, user, locations):
        """Check if symlinks for project and scratch exist, if not create"""
        import mysubprocess as subprocess
        import logging
        import os

        log = logging.getLogger('symlinks')
        lnbin = '/bin/ln'

        if not self.home_dir_exists(user):
            self.slackf += "Creating symlinks for `{}` because homedir doesn't exist\n".format(user)
            return False

        for loc in locations:
            # Create the paths for the src dir and dest symlink
            dirpath = os.path.join(loc, proj)
            linkpath = os.path.join(self.mnt, user, proj)
            if "/scratch" in loc:
                linkpath += "_scratch"

            # If the symlink doesn't exist, create it, wrapped by try/except
            if os.path.islink(linkpath):
                ownership = self.get_file_ownership(linkpath)
                if ownership != ('root', 'root'):
                    log.debug("Symlink not owned by root:root {}".format(linkpath))
                    self.slackw += "\nSymlink not owned by `root:root` `{}`".format(linkpath)

                    # cmd = ['/bin/chown', 'root:root', linkpath]
                    # try:
                    #     retcode = subprocess.call(cmd)
                    # except Exception as e:
                    #     log.exception(e)
                    #     raise
                    # finally:
                    #     log.info("Process {} returned {}".format(" ".join(cmd),
                    #                                      retcode))
                continue
            else:
                lncomm = [lnbin, '-s', dirpath, linkpath]
                log.info("Symlink {} does not exist".format(linkpath))
                try:
                    retcode = subprocess.call(lncomm)
                    self.slackg += "Creating `{}`\n".format(linkpath)
                except Exception as e:
                    log.exception(e)
                    self.slackf += "Creating `{}`\n".format(linkpath)
                    raise
                finally:
                    log.info("Process {} returned {}".format(" ".join(lncomm),
                                                             retcode))
        return True

    def get_user_groups(self, user, configdir):
        import os
        import yaml
        from mercldap.ldap import Client as LdapClient
        from mercallocations.allocations import AllocationsClient

        with open(os.path.join(configdir, 'ldapconfig.yml')) as filehandle:
            ldapconfig = yaml.load(filehandle.read())
        with open(os.path.join(configdir, 'allocationsconfig.yml')) as filehandle:
            allocconfig = yaml.load(filehandle.read())

        allocsClient = AllocationsClient(allocconfig['sheetid'],
                                         allocconfig['sheetname'],
                                         secret_file=os.path.join(configdir,
                                                                  'client_secret.json'))
        allocsClient.authorize()
        allocsClient.load()

        ldapClient = LdapClient(**ldapconfig)

        userprojs = [p['raw_attributes']['cn'] for p in ldapClient.getUsersProjects(user)]
        validprojs = [p for p in allocsClient.get_projects()]
        projs = [p for p in userprojs if str(p[0].decode('UTF-8')) in validprojs]
        return projs

    def copy_skeleton(self):
        pass

    class UserHome(object):
        def __init__(self, mnt, username,
                     skelpath='/usr/local/hpcusr/latest/skel/skel/'):
            self.mnt = mnt
            self.homepath = os.path.join(self.mnt, username)
            self.sshpath = os.path.join(self.homepath, ".ssh")
            self.idrsapath = os.path.join(self.sshpath, "id_rsa")
            self.authfile = os.path.join(self.sshpath, "authorized_keys")
            self.publickey = ".".join((self.idrsapath, "pub"))
            self.log = logging.getLogger('mgid.userhome')
            self.pubkeycontents = ''
            self.authkeycontents = ''
            self.username = username
            self.user_attrs = pwd.getpwnam(self.username)
            self.skelpath = skelpath

        def check_user_home(self):
            if os.path.islink(self.homepath):
                return True
            else:
                return os.path.exists(self.homepath)

        def check_user_ssh(self):
            if os.path.islink(self.homepath):
                return True
            else:
                return os.path.exists(self.sshpath)

        def check_user_idrsa(self):
            if os.path.islink(self.homepath):
                return True
            else:
                return os.path.exists(self.idrsapath)

        def check_user_auth(self):
            if os.path.islink(self.homepath):
                return True
            else:
                return os.path.exists(self.authfile)

        def check_user_publickey(self):
            if os.path.islink(self.homepath):
                return True
            else:
                return os.path.exists(self.publickey)

        def check_user_keys(self):
            if os.path.islink(self.homepath):
                return True

            if self.check_user_publickey() and self.check_user_auth():
                try:
                    with open(self.publickey, 'r') as pubkey:
                        self.pubkeycontents = pubkey.readlines()
                    with open(self.authfile, 'r') as authkey:
                        self.authkeycontents = authkey.readlines()

                    # Determine which keys are in both files
                    res = set(self.pubkeycontents).intersection(self.authkeycontents)

                    # If there aren't any common keys, return False
                    if len(res) == 0:
                        self.log.debug("Key not present in {}".format(
                            self.authfile
                        ))
                        return False
                    else:
                        return True
                except Exception as e:
                    self.log.critical("Could not check keys")
                    self.log.exception(e)
                    raise
            else:
                return False

        def check_user_permissions(self):
            """Checks that home dir has correct permissions, and that ssh keys
            aren't open"""
            # TODO: Implement this function
            return

        def create_dir(self, targetpath):
            """Generic method to create a folder"""
            self.log.debug("Creating dir {}".format(targetpath))
            try:
                cmd = ["mkdir", targetpath]
                mkdir = mysubprocess.Popen(cmd, stdout=mysubprocess.PIPE,
                                           stderr=mysubprocess.PIPE)
                stdout, stderr = mkdir.communicate()

                if stdout is not None:
                    self.log.debug("stdout for .ssh creation is {}:".format(stdout))
                if stderr is not None:
                    self.log.debug("stderr for .ssh creation is {}:".format(stderr))
                return True
            except Exception as e:
                self.log.critical("Unable to create dir {}".format(targetpath))
                self.log.exception(e)

        def create_user_sshdir(self):
            """Create the user's ssh directory"""
            self.create_dir(self.sshpath)

        def create_user_homedir(self):
            """Create the user's home directory (MonARCH)"""
            self.create_dir(self.homepath)

        def create_user_sshkey(self):
            """Create the user's id_rsa key for ssh between nodes"""
            cmd = "ssh-keygen -t rsa -N \"\" -f {}".format(self.idrsapath)
            self.log.debug(
                "Creating ssh key {} with command {}".format(self.idrsapath,
                                                             cmd)
            )

            try:
                keygen = mysubprocess.Popen([cmd], shell=True,
                                            stdout=mysubprocess.PIPE,
                                            stderr=mysubprocess.PIPE)
                stdout, stderr = keygen.communicate()
                if stdout is not None:
                    self.log.debug("stdout for key creation is {}:".format(stdout))
                if stderr is not None:
                    self.log.debug("stderr for key creation is {}:".format(stderr))
                return True
            except Exception as e:
                self.log.critical("ssh-keygen failed")
                self.log.exception(e)

        def create_user_auth(self):
            """Create the user's authorized_keys file"""

            cmd = ["cp", self.publickey, self.authfile]
            self.log.debug(
                "Creating authorized_keys file {} with command {}".format(self.authfile,
                                                                          cmd)
            )

            try:
                cp = mysubprocess.Popen(cmd, stdout=mysubprocess.PIPE,
                                        stderr=mysubprocess.PIPE)
                stdout, stderr = cp.communicate()
                if stdout is not None:
                    self.log.debug(
                        "stdout for authorized_keys creation is {}:".format(stdout)
                    )
                if stderr is not None:
                    self.log.debug(
                        "stderr for authorized_keys creation is {}:".format(stderr)
                    )
            except Exception as e:
                self.log.critical("authorized_keys cp failed")
                self.log.exception(e)

        def copy_idrsa_to_authkeys(self):
            # Check that the authorized_keys file exists, if not, create it
            if self.check_user_auth():
                pass
            else:
                self.create_user_auth()

            # If the id_rsa key is not in authorized_keys, add it
            if not self.check_user_keys():
                self.log.debug("Key not present in {}, adding".format(self.authfile))
                cmd = ["cat", self.publickey, ">>", self.authfile]
                try:
                    add_key = mysubprocess.Popen(cmd, stdout=mysubprocess.PIPE,
                                                 stderr=mysubprocess.PIPE)
                    stdout, stderr = add_key.communicate()
                    if stdout is not None:
                        self.log.debug("stdout for key copy is {}:".format(stdout))
                    if stderr is not None:
                        self.log.debug("stderr for key copy is {}:".format(stderr))
                    return True
                except Exception as e:
                    self.log.critical("Adding id_rsa to authorized_keys failed")
                    self.log.exception(e)
            else:
                return False

        def make_ssh_keys(self):
            """Creates the ssh key used to ssh between nodes on the cluster.
            Initially owned by root but permissions are changed in a subsequent
            step.
            """
            self.log.debug("Creating ssh keys for {}".format(self.username))

            # Check if ~/.ssh exists
            if self.check_user_ssh():
                res = True
            else:
                if self.check_user_home():
                    res = self.create_user_sshdir()
                    self.log.debug(res)
                else:
                    self.log.critical("Home directory {} does not exist?".format(self.homepath))

            # If ~/.ssh exists, check for id_rsa
            if res and self.check_user_idrsa():
                self.log.debug("id rsa exists")
                pass
            else:
                self.log.debug("id rsa does not")
                res = self.create_user_sshkey()

            if res and self.check_user_keys():
                self.log.debug("user keys exist")
                pass
            else:
                res = self.copy_idrsa_to_authkeys()

            self.log.debug("Finished creating ssh keys for {}".format(self.username))
            return res

        def copy_skeleton(self):
            """Copies the home directory skeleton into the user's home
            directory"""

            cmd = ['rsync', '-av', '--ignore-existing', self.skelpath, self.homepath]
            try:
                rsync = mysubprocess.Popen(cmd, stdout=mysubprocess.PIPE,
                                           stderr=mysubprocess.PIPE)
                stdout, stderr = rsync.communicate()
                if stdout is not None:
                    self.log.debug("stdout for skeleton rsync is: {}:".format(stdout))
                if stderr is not None:
                    self.log.debug("stderr for skeleton rsync is: {}:".format(stderr))
                return True

            except Exception as e:
                self.log.exeption(e)

        def recursive_chown(self, uid=None, gid=None):
            """Change the ownership of the user's home directory"""
            if uid is None:
                uid = self.user_attrs.pw_uid
            if gid is None:
                gid = self.user_attrs.pw_gid

            si = os.stat(self.homepath)
            if si.st_uid != uid or si.st_gid != gid:
                self.log.critical(
                    "Permission mismatch: user home dir {} uid {} should be {}, gid {} should be {}".format(
                        self.homepath, si.st_uid, uid, si.st_gid, gid)
                )

            self.log.debug("Recursively chowning path {}".format(self.homepath))
            try:
                cmd = ["chown", "-R", "{}:{}".format(uid, gid), self.homepath]
                chown = mysubprocess.Popen(cmd, stdout=mysubprocess.PIPE,
                                           stderr=mysubprocess.PIPE)
                stdout, stderr = chown.communicate()

                if stdout is not None:
                    self.log.debug("stdout for chown -R is {}:".format(stdout))
                if stderr is not None:
                    self.log.debug("stderr for chown -R is {}:".format(stderr))
                return True
            except Exception as e:
                self.log.critical("Unable to chown -R dir {}".format(self.sshpath))
                self.log.exception(e)

        def fix_home(self, uid, gid):
            try:
                self.copy_skeleton()
                os.chmod(self.homepath, 0o700)
                self.make_ssh_keys()
                self.recursive_chown(uid, gid)

            except Exception as e:
                self.log.exception(e)

            return

        def set_nfs_quota(self, quotas):
            """Set block usage and inode quotas for the user homedir"""

            cmd = ["/sbin/setquota", "-u", self.username, str(quotas['bsoft']),
                   str(quotas['bhard']), str(quotas['isoft']),
                   str(quotas['ihard']),
                   self.mnt]
            self.log.debug(
                "Setting quota for {} with command {}".format(self.username,
                                                              cmd)
            )

            try:
                setquota = mysubprocess.Popen(cmd, stdout=mysubprocess.PIPE,
                                        stderr=mysubprocess.PIPE)
                stdout, stderr = setquota.communicate()
                self.log.debug(
                    "stdout for setquota is {}:".format(stdout)
                )
                self.log.debug(
                    "stderr for setquota is {}:".format(stderr)
                )
            except Exception as e:
                self.log.critical("setquota failed")
                self.log.exception(e)

        def check_nfs_quota(self):
            """Check the quota for the user"""
            cmd = ["quota", "-u", self.username]
            try:
                getquota = mysubprocess.Popen(cmd, stdout=mysubprocess.PIPE,
                                              stderr=mysubprocess.PIPE)
                stdout, stderr = getquota.communicate()
                self.log.debug(
                    "stdout for quota is {}:".format(stdout)
                )
                self.log.debug(
                    "stderr for quota is {}:".format(stderr)
                )
            except Exception as e:
                self.log.critical("get quota failed")
                self.log.exception(e)
            return stdout

        def check_ownership(self, target):
            """Check that the uid and gid are correct for the target"""
            log = logging.getLogger('mgid.permissions')

            # Fetch uid and gid of owner of target, compare to user
            dir_attrs = os.stat(target)
            owner_correct = self.user_attrs.pw_uid == dir_attrs.st_uid
            group_correct = self.user_attrs.pw_gid == dir_attrs.st_gid

            log.debug("Ownership of {} correct? {}".format(target,
                                                           owner_correct & group_correct))

            return owner_correct & group_correct

        def mark_folder(self):
            """Add a .mgid file to the homepath to signal the rest of the
            provisioning process"""
            log = logging.getLogger('mgid.marker')
            with open(os.path.join(self.homepath, '.mgid'), 'w') as f:
                log.debug(os.path.join(self.homepath, '.mgid'))

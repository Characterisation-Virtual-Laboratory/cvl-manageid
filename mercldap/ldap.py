"""
This module implements an ldap interface with simplified calls for the HPC getProjectNames
"""
import ldap3
import logging

class Client(object):
    """
    Interaction with ldap is done through the client class
    This object is not "thread safe". Specifically using the same object from
    two threads or different objects from two threads will probably succeed,
    but if createUser is called twice from two different threads, it might
    create users with the same uidNumber (which would be bad) therefore please
    use a mutex around calls to createUser
    """
    def __init__(self,ldapURI=None,user=None,passwd=None,cafile=None,ldapAccountBase=None,*args,**kwargs):
        self.ldapURI=ldapURI
        self.user=user
        self.passwd=passwd
        self.cafile=cafile
        self.ldapAccountBase = ldapAccountBase
        self.conn=None
        self.ldapBase='dc=erc,dc=monash,dc=edu,dc=au'
        self.log=logging.getLogger('mgid.ldap')

    def __repr__(self):
        return("<mercldap.Client {}>".format(self.ldapURI))

    def __connect(self):
        """
        The __connect call will cause you to authenticate to the ldap server and be
        ready to make changes
        """
        tls_configuration = ldap3.Tls(validate=2, version=3,ca_certs_file=self.cafile)
        if isinstance(self.ldapURI,list):
            servers = [ ldap3.Server(uri,port=636,use_ssl=True, tls=tls_configuration,get_info=ldap3.ALL) for uri in self.ldapURI]
            self.conn=ldap3.Connection(servers,user=self.user,password=self.passwd,raise_exceptions=True)
        else:
            servers = ldap3.Server(self.ldapURI,port=636,use_ssl=True, tls=tls_configuration,get_info=ldap3.ALL)
            self.conn=ldap3.Connection(servers,user=self.user,password=self.passwd,raise_exceptions=True)
        if not self.conn.bind():
            raise Exception("error in bind {}".format(self.conn.result))


    def _createOu(self,ouName):
        if self.conn == None:
            self.__connect()
        self.conn.add('ou={},{}'.format(ouName,self.ldapBase),'organizationalUnit')

    def _removeOu(self,ouName):
        if self.conn == None:
            self.__connect()
        self.conn.delete('ou={},{}'.format(ouName,self.ldapBase))

    def _getMaxGidNumber(self):
        if self.conn == None:
            self.__connect()
        entries=self.conn.extend.standard.paged_search(self.ldapBase,'(gidNumber=*)',attributes=['gidNumber'],paged_size=100)
        gidlist = [ int(x['attributes']['gidnumber']) for x in entries ]
        # 65534 is the Nobody group, we shouldn't have gids above this
        return max( filter(lambda x: x < 65534, gidlist) )

    def _getMaxUidNumber(self):
        if self.conn == None:
            self.__connect()
        entries=self.conn.extend.standard.paged_search(self.ldapBase,'(uidNumber=*)',attributes=['uidNumber'],paged_size=100)
        maxuser = max( entries, key=lambda x: int(x['attributes']['uidNumber']))
        return maxuser['attributes']['uidNumber']

    def createUser(self,user,attributes={},uidNumber=None,ou='Accounts'):
        """
        create a user,
        the only necessary argument is user (the username)
        it will raise http://ldap3.readthedocs.io/exceptions.html
        like LDAPEntryAlreadyExists if the username exists
        Obviously its assumed you will check for the existance of the username first
        but expect the exception in case of an extremely unlikely race condition
        Note also that I don't believe we enforce uidNumber uniqueness constraints
        """
        nogroup=65534 # I should look this up on the system, but I will assume nogroup is always 65534 for the moment
        defaultAttributes={'sn':' ', 'cn':' ','pwdAttribute':'userPassword','gidNumber':nogroup,'homeDirectory':'/home/{}'.format(user)}
        for attrib in defaultAttributes:
            if not attrib in attributes:
                attributes[attrib]=defaultAttributes[attrib]
        objectClasses = ['organizationalPerson','top','inetOrgPerson','person','shadowAccount','pwdPolicy','posixAccount']
        if self.conn == None:
            self.__connect()
        lle = len(list(self.search('(ou={})'.format(ou))))
        if lle == 0:
            self._createOu('users')
        if uidNumber  == None:
            uidNumber = self._getMaxUidNumber() + 1
        attributes['uidNumber'] = uidNumber
        self.conn.add('uid={},ou={},{}'.format(user,ou,self.ldapBase),object_class=objectClasses,attributes=attributes)

    def deleteUser(self,user):
        userdn=self.getUserDN(user)
        self.conn.delete(userdn)

    def modifyUser(self,user,attributes):
        userdn = self.getUserDN(user)
        changes={}
        for attrib in attributes:
            changes[attrib] = (ldap3.MODIFY_REPLACE,[attributes[attrib]])
        self.conn.modify(userdn,changes)

    def getUserDN(self,user):
        u = self.searchUser(user)
        if u != None:
            return u['dn']
        else:
            raise ldap3.core.exceptions.LDAPNoSuchObjectResult('the specified user does not exist')

    def createProjectWithAllocation(self,projectName,allocationUri,membersUri=None):
        attributes={}
        attributes['description']=['allocation={}'.format(allocationUri),'members={}'.format(membersUri)]
        #attributes['description']=['allocation={} members={}'.format(allocationUri,membersUri)]
        self.createProject(projectName,attributes=attributes)

    def createProject(self,projectName,gidNumber=None,attributes={},ou='collaborations'):
        if self.conn == None:
            self.__connect()
        lle = len(list(self.search('(ou={})'.format(ou))))
        if lle == 0:
            self._createOu(ou)
        if gidNumber  == None:
            gidNumber = self._getMaxGidNumber() + 1
        attributes.update({'cn':projectName,'member':'','gidNumber':gidNumber})
        try:
            self.conn.add('cn={},ou={},{}'.format(projectName,ou,self.ldapBase),object_class=['groupOfNames','top','auxPosixGroup'],attributes=attributes)
        except ldap3.core.exceptions.LDAPEntryAlreadyExistsResult:
            self.log.debug("Project already exists in LDAP, ignoring this exception")
        except Exception as e:
            self.log.exception(e)

    def addUser(self,projectName,userName,ou='collaborations'):
        filter1='( &(ou:dn:={})(cn={})(|(objectClass=posixGroup)(objectClass=auxPosixGroup)))'.format(ou,projectName)
        filter2='(&(uid={})(objectClass=posixAccount))'.format(userName)
        self._addNestedDN(filter1,filter2)

    def getProjects(self,ou='collaborations'):
        filter1='(&(ou:dn:={})(|(objectClass=posixGroup)(objectClass=auxPosixGroup)))'.format(ou)
        for p in self.search(filter1):
            yield p

    def getProjectNames(self,ou='collaborations'):
        for p in self.getProjects(ou):
            yield p['attributes']['cn'][0]

    def getUsers(self):
        filter1='(objectClass=posixAccount)'
        for p in self.search(filter1):
            yield p
    def getPeople(self):
        filter1='(objectClass=person)'
        for p in self.search(filter1):
            yield p

    def getUsersProjects(self,username,ou='collaborations'):
        userdn = self.getUserDN(username)
        filter1='(&(ou:dn:={})(&(objectClass=auxPosixGroup)(member={})))'.format(ou,userdn)
        for p in self.search(filter1):
            yield p

    def groupName(self,project):
        return project['attributes']['cn'][0]

    def username(self,user):
        return user['attributes']['uid'][0]

    def allocationUri(self,project):
        import re
        if 'description' not in project['attributes']:
            return None
        for description in project['attributes']['description']:
            m = re.search('allocation=(\S+)',description)
            if m:
                return m.group(1)
        return None

    def getOldMembers(self,projectName):
        p  = self.searchOldProject(projectName)
        return self.getDNMembers(p['dn'])

    def getMembers(self, projectName, ou=None):
        if ou is not None:
            p = self.searchProject(projectName, ou)
        else:
            p = self.searchProject(projectName)
        return self.getDNMembers(p['dn'])


    def getDNMembers(self, pdn):
        p=self.conn.extend.standard.paged_search(pdn, '(objectClass=*)',
                                                 paged_size=3, generator=False,
                                                 attributes=['*'])
        # New style "groupOfNames" objects store a full dn in the member attributes
        # Old style "posixGroup" objects store a username in the memberUid attributes
        if 'member' in p[0]['attributes']:
            for mdn in p[0]['attributes']['member']:
                if mdn == '':
                    continue
                # If the DN begins with uid= then it must be a user, if it
                # begins with anything else, its probably a group, which we can recurse
                if 'uid=' in mdn[0:4]:
                    yield mdn
                else:
                    yield from self.getDNMembers(mdn)
        # If we've encountered a memberUid attribute, we need to turn it back from
        # a username into a DN
        if 'memberUid' in p[0]['attributes']:
            for muid in p[0]['attributes']['memberUid']:
                yield self.searchUser(muid)

    def addManager(self, projectName, userName, ou='collaborations'):
        filter1='(&(ou:dn:={})(&(cn={})(objectClass=auxPosixGroup)))'.format(ou,projectName)
        filter2='(&(uid={})(objectClass=posixAccount))'.format(userName)
        self._addNestedManagerDN(filter1,filter2)

    def rmUser(self, projectName, userName, ou='collaborations'):
        filter1='(&(ou:dn:={})(&(cn={})(objectClass=auxPosixGroup)))'.format(ou,projectName)
        filter2='(&(uid={})(objectClass=posixAccount))'.format(userName)
        self._rmNestedDN(filter1,filter2)

    def addNestedGroup(self, projectName1, projectName2, ou='collaborations'):
        filterstring = '(&(ou:dn:={})(&(cn={})(objectClass=auxPosixGroup)))'
        filter1 = filterstring.format(ou, projectName1)
        filter2 = filterstring.format(ou, projectName2)
        self._addNestedDN(filter1, filter2)

    def rmNestedGroup(self, projectName1, projectName2, ou='collaborations'):
        filterstring = '(&(ou:dn:={})(&(cn={})(objectClass=auxPosixGroup)))'
        filter1 = filterstring.format(ou, projectName1)
        filter2 = filterstring.format(ou, projectName2)
        self._rmNestedDN(filter1, filter2)

    def _addNestedDN(self, filter1, filter2):
        import logging
        log = logging.getLogger('mgid.ldap')
        if self.conn == None:
            self.__connect()
        group = self.conn.extend.standard.paged_search(self.ldapBase, filter1,
                                                       paged_size=3,
                                                       generator=False)
        member = self.conn.extend.standard.paged_search(self.ldapBase, filter2,
                                                        paged_size=3,
                                                        generator=False)

        if len(group) == 0:
            raise Exception("Can't add user, no groups matched")
        if len(group) > 1:
            raise Exception("Can't add user, too many groups matched: {}".format(group))
        if len(member) > 1:
            raise Exception("Can't add user, too many members matched: {}".format(member))
        if len(member) == 0:
            raise Exception("Can't add user, user doesn't exist")
        for m in member:
            for g in group:
                log.debug('Adding member {} to {}'.format(m['dn'], g['dn']))
                self.conn.modify(g['dn'],{'member':[ldap3.MODIFY_ADD,[m['dn']]]})

    def _addNestedManagerDN(self,filter1,filter2):
        import logging
        log = logging.getLogger('mgid.ldap')
        if self.conn == None:
            self.__connect()
        group=self.conn.extend.standard.paged_search(self.ldapBase,filter1,paged_size=3,generator=False)
        member=self.conn.extend.standard.paged_search(self.ldapBase,filter2,paged_size=3,generator=False)
        if len(group) > 1:
            raise Exception("Can't add user, too many groups matched: {}".format(group))
        if len(member) > 1:
            raise Exception("Can't add user, too many members matched: {}".format(member))
        for m in member:
            for g in group:
                log.debug('Removing member {} from {}'.format(m['dn'], d['dn']))
                self.conn.modify(g['dn'],{'owner':[ldap3.MODIFY_ADD,[m['dn']]]})

    def _rmNestedDN(self,filter1,filter2):
        import logging
        log = logging.getLogger('mgid.ldap')
        if self.conn == None:
            self.__connect()
        group=self.conn.extend.standard.paged_search(self.ldapBase,filter1,paged_size=3,generator=False)
        member=self.conn.extend.standard.paged_search(self.ldapBase,filter2,paged_size=3,generator=False)

        if len(group) == 0:
            raise Exception("Can't remove user, no groups matched")
        if len(group) > 1:
            raise Exception("Can't remove user, too many groups matched: {}".format(group))
        if len(member) > 1:
            raise Exception("Can't remove user, too many members matched: {}".format(member))
        if len(member) == 0:
            raise Exception("Can't remove user, user doesn't exist")
        for m in member:
            for g in group:
                log.debug('Removing member {} from {}'.format(m['dn'], g['dn']))
                self.conn.modify(g['dn'],{'member':[ldap3.MODIFY_DELETE,[m['dn']]]})

    def setUserPassword(self,username,passwd):
        """
        This function should be self explanatory
        given a username and a cleartext password, update the ldap entry
        """
        user = self.searchUser(username)
        from ldap3 import HASHED_SALTED_SHA
        from ldap3.utils.hashed import hashed
        hashed_password = hashed(HASHED_SALTED_SHA, passwd)
        self.conn.modify(user['dn'], {'userPassword': [(ldap3.MODIFY_REPLACE,[hashed_password])]})

    def lockAccount(self,user):
        if self.conn == None:
            self.__connect()
        ldapfilter='(&(uid={})(objectClass=posixAccount))'.format(user)
        users=self.conn.extend.standard.paged_search(self.ldapBase,ldapfilter,paged_size=3,generator=False)
        if len(users) > 1:
            raise Exception("Can't lock user, too many users matched: {}".format(users))
        if len(users) == 0:
            raise Exception("Can't lock user, no used matched filter: {}".format(ldapfilter))
        for u in users:
            self.conn.modify(u['dn'],{'pwdAccountLockedTime':[ldap3.MODIFY_REPLACE,['000001010000Z']]})

    def unlockAccount(self,user):
        if self.conn == None:
            self.__connect()
        ldapfilter='(&(uid={})(objectClass=posixAccount))'.format(user)
        users=self.conn.extend.standard.paged_search(self.ldapBase,ldapfilter,paged_size=3,generator=False)
        if len(users) > 1:
            raise Exception("Can't lock user, too many users matched: {}".format(users))
        if len(users) == 0:
            raise Exception("Can't lock user, no used matched filter: {}".format(ldapfilter))
        for u in users:
            self.conn.modify(u['dn'],{'pwdAccountLockedTime':[ldap3.MODIFY_REPLACE,[]]})



    def deleteProject(self,projectName,ou='collaborations'):
        if self.conn == None:
            self.__connect()
        self.conn.delete('cn={},ou={},{}'.format(projectName,ou,self.ldapBase))

    def searchProject(self,projectName,ou='collaborations'):
        entries = self.search('(cn={})'.format(projectName),base='ou={},{}'.format(ou,self.ldapBase))
        le = list(entries)
        if len(le) > 1:
            raise Exception("too any projects matched")
        if len(le) == 0:
            return None
        return le[0]

    def searchUser(self,user):
        entries = self.search('(&(uid={})(objectClass=posixAccount))'.format(user))
        le = list(entries)
        if len(le) > 1:
            raise Exception("too any users matched")
        if len(le) == 0:
            return None
        return le[0]

    def getUserByMail(self,mail):
        entries = self.search('(&(mail={})(objectClass=posixAccount))'.format(mail))
        le = list(entries)
        if len(le) > 1:
            raise Exception("too any users matched")
        if len(le) == 0:
            return None
        return le[0]

    def search(self, myfilter, base=None):
        """
        Use this method with an appropriate filter to determine if a username already exists
        Be aware it returns a generator. If the generator is not empty then the username exists
        """
        if base == None:
            base = self.ldapBase
        if self.conn == None:
            self.__connect()
        entries=self.conn.extend.standard.paged_search(base,myfilter,paged_size=100,attributes=['*'])
        return entries

    def generate_all_uids(self, mail, cn, suffix=''):
        """
        A very basic implementation of the username option rules
        note that this function does not filter for existing usernames
        note also that it accepts cn (common name) and attempts to split on
        spaces
        :param mail: the email address of the user
        :param cn: the common name of the user
        :param suffix: the suffix to append
        :return: a list of possible usernames, may already exist
        """

        import re
        if cn is None:
            import logging
            log = logging.getLogger('mgid.ldap')
            log.error('CN is not set')
            return []

        # Force usernames to be a minimum of 3 characters in length, beginning
        # with a letter (not a number)
        username_re = re.compile(
            r'^[a-z_]([a-z0-9_-]{2,31}|[a-z0-9_-]{2,30}\$)$'
        )
        names = cn.lower().split(' ')
        uids = []

        # First initial, all other names concatenated
        uid = names[0][0]
        for name in names[1:]:
            uid = uid + name
        uid += suffix
        if username_re.match(uid):
            uids.append(uid)

        # All names bar surname concatenated, first initial of final name
        uid = ''
        for name in names[0:-1]:
            uid = uid + name
        uid = uid + names[-1][0]
        uid += suffix
        if username_re.match(uid):
            uids.append(uid)

        # All names concatenated
        uid = ''
        for name in names:
            uid = uid + name
        uid += suffix
        if username_re.match(uid):
            uids.append(uid)

        # All names bar first name concatenated
        uid = ''
        for name in names[1:]:
            uid = uid + name
        uid += suffix
        if username_re.match(uid):
            uids.append(uid)

        # First name only
        uid = ''
        for name in names[0:1]:
            uid = uid + name
        uid += suffix
        if username_re.match(uid):
            uids.append(uid)

        return uids

    def get_possible_usernames(self, mail, cn, suffix=''):
        """
        Get only the acceptable options for a username based on generating all
        possible usernames and then filtering out the ones that are already
        taken. Note that a possible race condition exists here.
        :param mail:
        :param cn:
        :param suffix:
        :return:
        """

        possible_uids = self.generate_all_uids(mail, cn, suffix)
        for test_uid in possible_uids:
            search_str = "(&(uid={})(objectClass=posixAccount))".format(test_uid)
            users = list(self.search(search_str))
            # If a user already exists, skip this posix uid by continuing
            if len(users) > 0:
                continue
            yield test_uid

    def possible_username_handler(self, mail, cn, suffix='', min_length=3):
        """
        A handler to ensure that a minimum of 3 username options are returned
        each time a new username is requested
        :param mail:
        :param cn:
        :param suffix:
        :param min_length:
        :return:
        """

        # Loop through username generation until you have min_length username
        # options
        uids = set(self.get_possible_usernames(mail, cn))
        suffix = 1
        while len(uids) < min_length:
            uids = uids.union(
                set(
                    self.get_possible_usernames(
                        mail, cn, '{0:04d}'.format(suffix))
                )
            )
            suffix += 1

        return list(uids)

    def add_acl_user(self, projectName, userName, ou='aclgroups'):
        filter1='(&(ou:dn:={})(&(cn={})(objectClass=groupOfNames)))'.format(ou,projectName)
        filter2='(&(uid={})(objectClass=posixAccount))'.format(userName)
        self._addNestedDN(filter1, filter2)

    def remove_acl_user(self, projectName, userName, ou='aclgroups'):
        filter1='(&(ou:dn:={})(&(cn={})(objectClass=groupOfNames)))'.format(ou,projectName)
        filter2='(&(uid={})(objectClass=posixAccount))'.format(userName)
        self._rmNestedDN(filter1, filter2)

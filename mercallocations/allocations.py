class AllocationsClient():
    def __init__(self,sheetid,sheetname,secret_file='client_secret.json'):
        import logging
        self._data = None
        self._columnnames = None
        self.service = None
        self.sheetid = sheetid
        self.sheetname = sheetname
        self.secret_file = secret_file
        self.columnnamesrange = None
        self.datarange = None
        self.data = []
        self.project_whitelist = ['rdsmtest', 'mmi-llsm', 'uow-mh-cryoem',
                                  'training', 'ptesting', 'pMOSP', 'pMERC',
                                  'mips-cryoem']
        self.log = logging.getLogger('mgid.alloc')

    def authorize(self,readonly=True):
        import httplib2
        import os

        from apiclient import discovery
        from oauth2client import client
        from oauth2client import tools
        from oauth2client.file import Storage
        from oauth2client.service_account import ServiceAccountCredentials

        # SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        if readonly:
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        else:
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


        credentials = ServiceAccountCredentials. \
                      from_json_keyfile_name(self.secret_file, scopes=SCOPES)
        http = credentials.authorize(httplib2.Http())
        discoveryurl = ('https://sheets.googleapis.com/$discovery/rest?'
                        'version=v4')
        self.service = discovery.build('sheets', 'v4', http=http,
                                       discoveryServiceUrl=discoveryurl)

    def load(self):
        """Retrieve data from the spreadsheet. Assumes
        1. The first row contains column names
        2. The lastcol is <Z"""
        from googleapiclient.errors import HttpError

        lastcol = 'Z'
        result = self.service.spreadsheets().sheets()
        self.columnnamesrange = '{}!A1:{}1'.format(self.sheetname, lastcol)
        self.datarange = '{}!A2:{}'.format(self.sheetname, lastcol)

        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.sheetid, range=self.columnnamesrange).execute()
        self._columnnames = result.get('values', [])[0]
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheetid, range=self.datarange).execute()
        except HttpError as e:
            if e.resp.status in [500, 503]:
                self.log.info("Google Sheets is experiencing an issue, "
                              "try again later")
            else:
                self.log.exception(e)
                self.log.critical('Google Sheets encountered an error, exiting')
            import sys
            sys.exit(-1)
        except Exception as e:
            self.log.exception(e)
            self.log.critical('Google Sheets encountered an error, exiting')
            import sys
            sys.exit(-1)

        self._data = result.get('values', [])

    def _get_project_row(self, projname):
        for i in range(0, self.nrows()):
            if self._get_project_name(i) == projname:
                return i

    def _get_project_name(self, row):
        colnum = self._columnnames.index('Project Code')
        return self._data[row][colnum]

    def _set_members(self, value, row):
        colnum = self._columnnames.index('8. Project members (Email addresses)')
        self._data[row][colnum] = value

    def _get_raw_members(self, row):
        colnum = self._columnnames.index('8. Project members (Email addresses)')
        return self._data[row][colnum]

    def extract_emails(self, emstring):
        mail = []
        import re
        if emstring is None:
            return []
        mailre = re.compile(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)")
        for tok in emstring.split(' '):
            match = mailre.search(tok)
            if match:
                mail.append(match.group(1).lower())
        if emstring is not '' and mail is []:
            print("Couldn't find an email in {}".format(emstring))
        return mail

    def get_project_member_emails(self, projname):
        emails = self.get_field(projname,'8. Project members (Email addresses)')
        return self.extract_emails(emails)

    def get_project_leader_emails(self,projname):
        emails = self.get_field(projname,'6. Project leader (Full Name and Email address)')
        return self.extract_emails(emails)

    def get_project_parent(self, projname):
        return self.get_field(projname,'Parent Project')

    def _is_approved(self, rownum):
        colnum = self._columnnames.index('Status')
        try:
            status = self._data[rownum][colnum]
        except IndexError:
            status = None
        return status == 'Approved' or status == 'Ongoing'

    def _is_valid_projectname(self, rownum):
        import re
        projname = self._get_project_name(rownum)
        validre = re.compile(r"[a-zA-Z]+[0-9]+")
        match = validre.match(projname)
        if match:
            return True
        # Added a list to circumvent the regex constraint
        if projname in self.project_whitelist:
            return True
        return False

    def _is_valid(self, rownum):
        if not self._is_valid_projectname(rownum):
            return False
        # Add additional validation checks here if you like
        return True

    def get_all_project_details(self):
        for i in range(0, self.nrows()):
            yield self._data[i][:]

    def get_all_projects(self):
        for i in range(0, self.nrows()):
                yield self._get_project_name(i)

    def get_invalid_projects(self):
        for i in range(0, self.nrows()):
            if self._is_approved(i) and not self._is_valid(i):
                yield self._get_project_name(i)

    def get_projects(self):
        for i in range(0, self.nrows()):
            if self._is_approved(i) and self._is_valid(i):
                yield self._get_project_name(i)

    def get_field(self,projname,fieldname):
        row = self._get_project_row(projname)
        try:
            colnum = self._columnnames.index(fieldname)
        except ValueError as e:
            self.log.exception("Error, the field {} is not in the list {}.".format(fieldname,self._columnnames))
            self.log.exception("If you're seeing this error you probably need to edit mercallocations/allocations.py to make the field names match with the spreadsheet")
            raise(e)
        if colnum is None or row is None:
            return None
        return self._data[row][colnum]

    def get_for(self,projname):
        return self.get_field(projname,'FOR Code')

    def get_soe(self,projname):
        return self.get_field(projname,'SEO Code')

    def get_expiry(self,projname):
        start = self.get_field(projname,'Timestamp')
        duration = self.get_field(projname,'7. Project duration')
        return "start: {} duration: {}".format(start,duration)

    def getrow(self, rownum):
        return self._data[rownum]

    def updaterow(self, rownum, data):
        self._data[rownum] = data

    def nrows(self):
        return len(self._data)

    def save(self):
        body = {'values':self._data}
        self.service.spreadsheets().values().update(
            spreadsheetId=self.sheetid, range=self.datarange, body=body, valueInputOption="RAW"
        ).execute()

    def is_mxproject(self,projname):
        row = self._get_project_row(projname)
        colnum = self._columnnames.index('5.2 Is this work related to MX Collaborative Access Program (CAP)?')
        if self._data[row][colnum] == 'Yes':
            return True

        return False

    def description(self,projname):
        return self.get_field(projname,'Project Description ')

    def title(self,projname):
        return self.get_field(projname,'Project Title')

    def is_cryoproject(self,projname):
        if self.get_field(projname,'5.1 Is this work related to CRYO-EM?') == 'Yes':
            return True
        return False

    def is_cvlproject(self,projname):
        return self.get_project_parent(projname) == 'p004'

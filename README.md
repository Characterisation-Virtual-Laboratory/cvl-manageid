# Development

Code in python3. You should be able to create a venv (python3 -m venv testing ; ./testing/bin/activate) and pip install (pip install git+ssh://git@gitlab.erc.monash.edu.au:hpc-team/smux.git)

pip install pytest and use pytest test/tests.py

# Architecture

## Create/Update a Project

### Arguments

    1. POSIX Group name (project code)
    2. RT Ticket number (optional)
    3. Slurm Parent Project (prompt p01-p05)
    4. Project Type (provides default values)
    5. Slurm Shares (default 1 share of the parent)
    6. FS allocations
    7. FOR SOE and description (for creation, not needed for update?)

### Actions

    1. Create an LDAP entry for the project
    2. Create Directories on lustre and set quota
    3. Use sacctmgr to create the account under the correct parent
    4. Log the creation/update somewhere (json file, syslog, spreadsheet, other)

## Create/Update a user

### Arguments

    1. Username
    2. email address
    3. Other optional details

### Actions

    1. Create user in ldap

## Add/remove a use from a group

### Arguments

    1. Group name
    2. Username
    3. RT Ticket (need to track that we checked with the PI)

### Actions

Guess

## Cron Tasks

    1. Update slurm accounts with users based on group memebership data
    2. Update symlinks in homedirs based on group memebership data

## Password Change

I'm not sure if this should be in this repo
I imagine a python HTTP API accessable with either Shibboleth (AAF) or Google (OpenID Connect) Auth. Either of these email addresses provides a trusted email address. If the users email matches the value from the Auth method, they can set a password.

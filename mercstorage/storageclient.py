"""Methods for interacting with generic storage systems

The rationale for this change: some clusters have project storage that is 
not Lustre (CVL@UWA for example). Using a generic storageClient with common 
methods, instead of a bunch of if-else statements to cover all possible cases, 
simplifies code that uses these clients. For example:

from mercstorage import storageClient as genericStorageClient

if args.cluster == 'm3' or args.cluster == 'monarch':
    storageClient = lfsClient
else:
    storageClient = genericStorageClient
# Stuff
# Test for provisioning status
for mntpoint in storageClients.get_mount_points():
    if storageClient.exists(projcode, mntpoint):
        slack['ok'].add(project)
    else:
        slack['failed'].add(project) 
"""


class StorageClient(object):
    """Class holds empty methods for interacting with storage systems -
    should be overridden"""

    def __init__(self, config):
        self._config = config

    def create(self):
        raise NotImplementedError

    def exists(self):
        raise NotImplementedError

    def get_mount_points(self):
        raise NotImplementedError

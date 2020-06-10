import manageid

def make_map(args):
    clients = manageid.get_clients_dict(args,clients={'ldap':True})
    search_str = "(&(objectClass=posixAccount)(mail=*)(uid=*))"
    for user in clients['ldap'].search(search_str):
        for mail in user['attributes']['mail']:
            print("{}: {}".format(mail,user['attributes']['uid'][0]))


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--configdir')

    args = parser.parse_args()

    make_map(args)

if __name__ == '__main__':
    main()

# This script is used to backup and restore RBAC users in a couchbase cluster with version >5.0
# To use this script to backup an existing clusters RBAC users enter the required arguments on a machine that has
# REST access to the cluster to be backed up and run the script. You will have to manually update the password for each
# entry in the file you created as the passwords are not recoverable. They passwords will be set to a default of
# "password". From here you can update the cluster list to include other couchbase clusters you would like to
# add these RBAC users to. Once the passwords and cluster list are updated you can run the script with the restore flag.
# As long as you are using the default file location restore is the only flag you need. The script will push the RBAC
# settings to all of the clusters in the list. (The location the script is run from must have REST access to the
# clusters to have users restored)

# example backup: python rbac.py --username Administrator --password password --cluster localhost:8091 --backup
# example restore: python rbac.py --restore

import argparse
import json

import requests


def backup():
    url = 'http://' + args.host + '/settings/rbac/users'
    print("my url : " + str(url))
    response = requests.get(url, auth=(args.username, args.password))
    if response.status_code == 200:
        rbacInfo = {}
        clusterInfo = {}
        clusterInfo['cluster'] = []
        cluster = {}
        cluster['address']= args.host
        cluster['userName'] = args.username
        cluster['password'] = args.password
        clusterInfo['cluster'].append(cluster)
        rbacInfo['clusterInfo'] = clusterInfo
        rbacInfo['userInfo'] = []
        try:
            records = json.loads(response.text)
            for entry in records:
                entry['password'] = "password"
                rbacInfo['userInfo'].append(entry)
            with open(args.rbacFile, "w") as f:
                f.write(json.dumps(rbacInfo, indent=4, sort_keys=True))
                print('Successfully backed up RBAC data to: [%s]' % args.rbacFile)
        except Exception as e:
            print('Error' + str(e.args))
    else:
        print('Http requests {url} failed: {response.status_code}')
 
def restore():
    try:
        with open(args.rbacFile) as f:
            rbacData = json.loads(f.read())
            for cluster in rbacData['clusterInfo']['cluster']:
                print(cluster)
                for user in rbacData['userInfo']:
                    url = 'http://{}/settings/rbac/users/{}/{}'.format(cluster['address'],user['domain'], user['id'])
                    params = {}
                    if 'name' in user:
                        params['name'] = user['name']
                    roles = list()
                    for role in user['roles']:
                        if 'bucket_name' in  role:
                            roles.append('{}[{}]'.format(role['role'], role['bucket_name']))
                        else:
                            roles.append(role['role'])
                    params['roles'] = ','.join(roles)

                    output = 'Successfully restored user {}'.format(user['id'])
                    params["password"] = user['password']
                    response = requests.put(url, params, auth=(cluster['userName'], cluster['password']))
                    if response.status_code == 200:
                        print(output + json.dumps(params))
                    else:
                        print(params)
                        print(f'Http requests {url} failed: {response.status_code} {response.text}')

    except Exception as e:
                print('Error:' + str(e.args))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='To backup and Restore Couchbase RBAC Users')
    parser.add_argument('--username', metavar='<username>', dest='username', default='Administrator',
                        help='The username for the Couchbase cluster')
    parser.add_argument('--password', metavar='<password>', dest='password', default='password',
                        help='The password for the Couchbase cluster')
    parser.add_argument('--cluster', metavar='<hostname:port>', dest='host', default='localhost:8091',
                        help='The hostname of the Couchbase cluster')
    parser.add_argument('--file', metavar='<file>', dest='rbacFile', default='rbac.json',
                        help='The file to read and write the RBAC data to')

    #Action backup or restore
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--backup', action='store_true', default=False)
    action.add_argument('--restore', action='store_true', default=False)

    args = parser.parse_args()

    print("my host : " + str(args.host))
    if args.backup:
        backup()
    elif args.restore:
        restore()

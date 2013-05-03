#!/usr/bin/env python
#Copyright 2013 Matthew Thode

#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.


from __future__ import print_function
import pyrax
import argparse
from time import sleep



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Builds servers')
    parser.add_argument('--user', '-u', required=True,
                        help='Set\'s the account user')
    parser.add_argument('--apikey', '-k', required=True,
                        help='Set\'s the account API key')
    parser.add_argument('--region', '-r', required=True,
                        help='Set\'s the account region')
    args = parser.parse_args()

    pyrax.set_credentials(username=args.user, api_key=args.apikey,
                          region=args.region)
    if args.region == 'ORD':
        cs = pyrax.connect_to_cloudservers("ORD")
        cdb = pyrax.connect_to_cloud_databases("ORD")
        cnw = pyrax.connect_to_cloud_networks("ORD")
        cbs = pyrax.connect_to_cloud_blockstorage("ORD")
        dns = pyrax.connect_to_cloud_dns("ORD")
        lbs = pyrax.connect_to_cloud_loadbalancers("ORD")
    elif args.region == 'DFW':
        cs = pyrax.connect_to_cloudservers("DFW")
        cdb = pyrax.connect_to_cloud_databases("DFW")
        cnw = pyrax.connect_to_cloud_networks("DFW")
        cbs = pyrax.connect_to_cloud_blockstorage("DFW")
        dns = pyrax.connect_to_cloud_dns("DFW")
        lbs = pyrax.connect_to_cloud_loadbalancers("DFW")
    elif args.region == 'LON':
        cs = pyrax.connect_to_cloudservers("LON")
        cdb = pyrax.connect_to_cloud_databases("LON")
        cnw = pyrax.connect_to_cloud_networks("LON")
        cbs = pyrax.connect_to_cloud_blockstorage("LON")
        dns = pyrax.connect_to_cloud_dns("LON")
        lbs = pyrax.connect_to_cloud_loadbalancers("LON")

    print('deleting servers')
    for server in cs.servers.list():
        server.delete()

    print('deleting server images')
    for image in cs.images.list():
        if hasattr(image, "server"):
            cs.images.delete(image.id)

    print('deleting all dns records')
    for domain in dns.list():
        domain.delete()

    print('deleting databases')
    for db in cdb.list():
        db.delete()

    print('waiting for servers to be deleted to delete networks and volumes')
    while not cs.servers.list() == []:
        sleep(10)

    print('deleting networks')
    for network in cnw.list():
        if network.id == '00000000-0000-0000-0000-000000000000' or \
                '11111111-1111-1111-1111-111111111111':
            continue
        network.delete()

    print('deleting volumes and volume snapshots')
    for vol in cbs.list():
        while not vol.list_snapshots() == []:
            vol.delete_all_snapshots()
            sleep(10)
        vol.delete()

    print('deleting load balancers')
    for lb in lbs.list():
        lb.delete()

    print('finished')
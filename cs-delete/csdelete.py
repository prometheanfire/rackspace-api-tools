#!/usr/bin/env python
#Copyright 2012 Matthew Thode

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
"""
Rackspace Cloud Servers Deletion Script
"""

import argparse
import sys
import json
import httplib


def auth(user=None, apikey=None, region=None):
    """
    Authenticate and return authentication details via returned dict
    """
    if args.endpoint == 'LON':
        authurl = 'lon.auth.api.rackspacecloud.com'
    else:
        authurl = 'auth.api.rackspacecloud.com'
    if args.password:
        jsonreq = json.dumps({'auth': {'passwordCredentials': {
                            'username': args.apiuser,
                            'password': args.password}}})
    else:
        jsonreq = json.dumps({'auth': {'RAX-KSKEY:apiKeyCredentials':
                            {'username': args.apiuser,
                            'apiKey': args.apikey}}})
    if args.veryverbose:
        print 'JSON REQUEST: ' + jsonreq

    #make the request
    connection = httplib.HTTPSConnection(authurl, 443)
    if args.veryverbose:
        connection.set_debuglevel(1)
    headers = {'Content-type': 'application/json'}
    connection.request('POST', '/v2.0/tokens', jsonreq, headers)
    json_response = json.loads(connection.getresponse().read())
    connection.close()

    #process the request
    if args.veryverbose:
        print 'JSON decoded and pretty'
        print json.dumps(json_response, indent=2)
    details = {}
    try:
        catalogs = json_response['access']['serviceCatalog']
        for service in catalogs:
            if service['name'] == 'cloudServers':
                for endpoint in service['endpoints']:
                    details['endpoint'] = endpoint['publicURL']
                    details['tenantid'] = endpoint['tenantId']
        details['token'] = json_response['access']['token']['id']
        if args.verbose:
            print 'Endpoint:\t', details['endpoint']
            print 'Tenant:\t\t', details['tenantid']
            print 'Token:\t\t', details['token']
    except(KeyError, IndexError):
        print 'Error while getting answers from auth server.'
        print 'Check the endpoint and auth credentials.'
    return details


def get_servers():
    """
    Gets a list of servers and returns it
    """
    global authdata
    endpoint = authdata['endpoint'].split('/')[2]
    serverlist = []
    connection = httplib.HTTPSConnection(endpoint, 443)
    filepath = ('/v1.0/' + authdata['tenantid'] +
                '/servers')
    if args.veryverbose:
        connection.set_debuglevel(1)
    while True:
        headers = {'X-Auth-Token': authdata['token']}
        try:
            connection.request('GET', filepath, '', headers)
            response = connection.getresponse()
            json_response = json.loads(response.read())
            if response.status == 401:
                authdata = auth()
                continue
        except httplib.BadStatusLine:
            print 'Failed connection, no data recieved.'
            print 'Closing connection and retrying.'
            connection.close()
            connection = httplib.HTTPSConnection(endpoint, 443)
            continue
        try:
            servers = json_response['servers']
            for server in servers:
                serverlist.append(server['id'])
        except Exception:
            print 'Error while parsing returned server list.'
        if args.verbose:
            print 'Number of Servers:\t', len(serverlist)
        if args.veryverbose:
            for i in serverlist:
                print 'Servers:\t' + str(i)
        #do the deletion
        connection.close()
        break
    return serverlist


def del_servers(serverlist):
    """
    Deletes a list of servers
    """
    global authdata
    endpoint = authdata['endpoint'].split('/')[2]
    connection = httplib.HTTPSConnection(endpoint, 443)
    filepath = ('/v1.0/' + authdata['tenantid'] + '/servers/')
    if args.veryverbose:
        connection.set_debuglevel(1)
    while True:
        headers = {'X-Auth-Token': authdata['token']}
        for server in serverlist:
            try:
                connection.request('DELETE', filepath + str(server),
                    '', headers)
                response = connection.getresponse()
                response.read()
                if response.status == 401:
                    authdata = auth()
                    continue
            except httplib.BadStatusLine:
                print 'Failed connection, no data recieved.'
                print 'Closing connection and retrying.'
                connection.close()
                connection = httplib.HTTPSConnection(endpoint, 443)
                continue
        connection.close()
        break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gets auth data via json')
    parser.add_argument('--apiuser', '-u', required=True, help='Api username')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Turn up verbosity to 10')
    parser.add_argument('--veryverbose', action='store_true',
                        help='Turn up verbosity to 11')
    parser.add_argument('--endpoint', '-e', required=True,
                        choices=['dfw', 'lon', 'ord'],
                        help='Sets the datacenter')
    parser.add_argument('--murder', action='store_true',
                        help=('Murder all objects and servers in an ' +
                            ' account'))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--apikey', '-a',  help='Account api key')
    group.add_argument('--password', '-p', help='Account password')

    args = parser.parse_args()
    args.endpoint = args.endpoint.upper()
    if args.veryverbose:
        args.verbose = True
        print parser.parse_args()
    global authdata
    if args.murder:
        authdata = auth()
        serverlist = get_servers()
        del_servers(serverlist)
    else:
        authdata = auth()

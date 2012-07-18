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
Rackspace Cloud Files Deletion Script
"""

import argparse
import sys
import datetime
import time
import json
import httplib
from urllib import quote
import multiprocessing


def cfauth(user=None, apikey=None, region=None):
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
    cfdetails = {}
    try:
        catalogs = json_response['access']['serviceCatalog']
        for service in catalogs:
            if service['name'] == 'cloudFiles':
                for endpoint in service['endpoints']:
                    if endpoint['region'] == args.endpoint:
                        if args.internal:
                            cfdetails['endpoint'] = endpoint['internalURL']
                        else:
                            cfdetails['endpoint'] = endpoint['publicURL']
                        cfdetails['tenantid'] = endpoint['tenantId']
            elif service['name'] == 'cloudFilesCDN':
                for endpoint in service['endpoints']:
                    if endpoint['region'] == args.endpoint:
                        cfdetails['cdnendpoint'] = endpoint['publicURL']
        cfdetails['token'] = json_response['access']['token']['id']
        if args.verbose:
            print 'CDN Endpoint:\t', cfdetails['cdnendpoint']
            print 'Endpoint:\t', cfdetails['endpoint']
            print 'Tenant:\t\t', cfdetails['tenantid']
            print 'Token:\t\t', cfdetails['token']
    except(KeyError, IndexError):
        print 'Error while getting answers from auth server.'
        print 'Check the endpoint and auth credentials.'
    return cfdetails


def get_containers():
    """
    Gets a list of containers, 10,000 at a time
    Can use Gigs of data if you have millions of containers
    """
    global authdata
    endpoint = authdata['endpoint'].split('/')[2]
    lastcontainer = ''
    containerlist = []
    runonce = True
    connection = httplib.HTTPSConnection(endpoint, 443)
    filepath = '/v1/' + authdata['tenantid'] + '/?limit=10000&format=json'
    if args.veryverbose:
        connection.set_debuglevel(1)
    while True:
        containerlist = []
        headers = {'X-Auth-Token': authdata['token']}
        if lastcontainer:
            filepath = filepath + '&marker=' + quote(lastcontainer)
        try:
            connection.request('GET', filepath, '', headers)
            response = connection.getresponse()
            json_response = json.loads(response.read())
            if runonce:
                containerlistsize = int(response.getheader(
                'X-Account-Container-Count'))
                if containerlistsize == 0:
                    print 'No containers to delete, exiting.'
                    sys.exit()
                print 'Need to enumerate', containerlistsize, 'containers.'
            if response.status == 401:
                authdata = cfauth()
                continue
        except httplib.BadStatusLine:
            print 'Failed connection, no data recieved.'
            print 'Closing connection and retrying.'
            connection.close()
            connection = httplib.HTTPSConnection(endpoint, 443)
            continue
        try:
            for names in json_response:
                containerlist.append(names['name'])
                print lastcontainer
        except Exception:
            print 'Error while parsing returned container list, exiting.'
            #sys.exit()
        if args.verbose:
            print 'Number of Containers:\t', len(containerlist)
        if args.veryverbose:
            for i in containerlist:
                print 'Container:\t' + i
        #do the deletion
        container_delete_pool = multiprocessing.Pool(args.cc)
        container_delete_pool.map(del_container, containerlist)
        container_delete_pool.close()
        container_delete_pool.join()
        if len(containerlist) >= containerlistsize:
            if len(containerlist) > containerlistsize:
                print ('Somehow got a list of containers that was greater ' +
                    'then where there when this program was started ' +
                    'Goint to continue under the assumption that ' +
                    'containers were added while we were getting the list ' +
                    'and you still want to delete them.')
            connection.close()
            break


def authtest():
    """
    Tests auth every minute to cdn and api endpoints FOREVER and \
    outputs a bunch of debug info.
    """
    print 'RUNNING AUTH TEST FOREVER, I ASSUME YOU KNOW WHAT YOU WANT'
    while True:
        print 'Auth time:\t\t', datetime.datetime.now()
        authdata = cfauth()
        headers = {'X-Auth-Token': authdata['token']}
        endpoint = authdata['endpoint'].split('/')[2]
        #api endpoint testing
        print 'API time:\t\t', datetime.datetime.now()
        connection = httplib.HTTPSConnection(endpoint, 443)
        connection.set_debuglevel(1)
        connection.request('HEAD', '/v1/' + authdata['tenantid'], '', headers)
        response = connection.getresponse()
        response.read()
        if response.status == 401:
            print 'apiFAILapiFAILapiFAILapiFAILapiFAILapiFAILapiFAILapiFAIL'
        connection.close()
        #cdn endpoint testing
        print 'CDN time:\t\t',  datetime.datetime.now()
        endpoint = authdata['cdnendpoint'].split('/')[2]
        connection = httplib.HTTPSConnection(endpoint, 443)
        connection.set_debuglevel(1)
        connection.request('GET', '/v1/' + authdata['tenantid'], '', headers)
        response = connection.getresponse()
        response.read()
        if response.status == 401:
            print 'cdnFAILcdnFAILcdnFAILcdnFAILcdnFAILcdnFAILcdnFAILcdnFAIL'
        connection.close()
        time.sleep(60)


def del_container_contents(container, objectlist, lastobject):
    """
    Deletes the contents of the passed container and object list
    """
    global authdata
    endpoint = authdata['endpoint'].split('/')[2]
    connection = httplib.HTTPSConnection(endpoint, 443)
    retry = True
    if args.veryverbose:
        connection.set_debuglevel(1)
    skipobjects = False
    if not skipobjects:
        for obj in objectlist:
            retry = True
            while retry:
                retry = False
                headers = {'X-Auth-Token': authdata['token'],
                    'Connection': 'Keep-Alive'}
                try:
                    connection.request('DELETE', '/v1/' +
                        authdata['tenantid'] + '/' + quote(container) + '/' +
                        quote(obj), '', headers)
                    response = connection.getresponse()
                    response.read()
                except httplib.BadStatusLine:
                    print 'Failed connection, no data recieved.'
                    print 'Closing connection and retrying.'
                    connection.close()
                    connection = httplib.HTTPSConnection(endpoint, 443)
                    retry = True
                    continue
                if response.status == 401:
                    authdata = cfauth()
                    retry = True
                    continue
            if obj == lastobject:
                break
    connection.close()


def del_container(container):
    """
    Gets the objects in a continer and deletes the objects and \
    the container via the del_container_contents function
    """
    from gevent.pool import Pool as gevent_pool
    from gevent import monkey
    monkey.patch_all()
    global authdata
    endpoint = authdata['endpoint'].split('/')[2]
    lastobject = ''
    container = container.encode('utf-8')
    objlistsize = 0
    objlistsent = 0
    runonce = True
    pool = gevent_pool(args.oc)
    while True:
        headers = {'X-Auth-Token': authdata['token'],
            'Connection': 'Keep-Alive'}
        connection = httplib.HTTPSConnection(endpoint, 443)
        if args.veryverbose:
            connection.set_debuglevel(1)
        objectlist = []
        filepath = '/v1/' + authdata['tenantid'] + '/' + \
                    quote(container) + '/?limit=10000&format=json'
        if lastobject:
            filepath = filepath + '&marker=' + quote(lastobject)
        if args.verbose:
            print filepath
        connection.request('GET', filepath, '', headers)
        response = connection.getresponse()
        if response.status == 401:
            print 'Auth data is bad or stale, getting auth data again'
            print authdata['token']
            authdata = cfauth()
            retry = True
            continue
        elif response.status == 404:
            print ('Tried to get data on a the container ' + container +
                ' that does not exist\n\tpossible contention issue, ' +
                'please wait and try again\n\tgoing on to the next ' +
                'container (if one exists)')
            break
        json_response = json.loads(response.read())
        if runonce:
            objlistsize = int(response.getheader(
                'X-Container-Object-Count'))
            if args.verbose:
                if objlistsize == 0:
                    print ('No objects to delete, proceeding to ' +
                        'container delete')
                elif objlistsize > args.oc * 10:
                    print ('Going to delete ' + str(objlistsize) +
                        ' objects from ' + container +
                        ' at about 100 per second (hopefully).')
                else:
                    print ('Going to delete ' + str(objlistsize) +
                        ' objects at about 3-4 per second as there ' +
                        'are less then ' + str(args.oc * 10) +
                        ' objects in ' + container + '.')
            runonce = False
        if args.veryverbose:
            if len(json_response) > 0:
                print json.dumps(json_response, indent=2)
            else:
                print 'No objects in container \'' + container + '\'.'
        try:
            if len(json_response) > 0:
                for obj in json_response:
                    objectlist.append(obj['name'].encode('utf-8'))
                lastobject = objectlist[-1]
                if args.verbose:
                    print lastobject
        except Exception:
            print 'Error while parsing returned object list.'
        #split objets into managable chunks
        if objlistsize == 0:
            pass
        elif (args.oc * 10) <= objlistsize <= (args.oc * 10000):
            listOfObjectLists = ([objectlist[i:i + args.oc]
                for i in range(0, len(objectlist), args.oc)])
            for sublist in listOfObjectLists:
                pool.apply_async(del_container_contents, args=(container,
                    sublist, sublist))
        else:
            pool.apply_async(del_container_contents, args=(container,
                objectlist, lastobject))
        pool.wait_available()
        objlistsent += len(objectlist)
        #check to see if we sent all the objects, if so, we wait for them
        #to be deleted and then perform the container delete.
        try:
            if objlistsent < objlistsize:
                continue
            elif objlistsent == objlistsize:
                pool.join()
                #sleep to help avoid contention issues, can probably be
                #lowered if used with container sizes that are tiny
                #(less then 500 objects)
                #time.sleep(15)
                #delete the container
                retry = True
                while retry:
                    retry = False
                    headers = {'X-Auth-Token': authdata['token'],
                        'Connection': 'Keep-Alive'}
                    connection = httplib.HTTPSConnection(endpoint, 443)
                    if args.veryverbose:
                        connection.set_debuglevel(1)
                    connection.request('DELETE',
                        '/v1/' + authdata['tenantid'] + '/' +
                        quote(container), '', headers)
                    response = connection.getresponse()
                    response.read()
                    if response.status == 401:
                        authdata = cfauth()
                        retry = True
                        continue
                    connection.close()
                break
            else:
                pool.join()
                raise
        except Exception:
            print ('Somehow, we tried to delete more objects then' +
                'existed in the container...\n' +
                'Not sending delete for container.')
            break
        finally:
            connection.close()

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
    parser.add_argument('--internal', '-i', action='store_true',
                        help='Use servicenet')
    parser.add_argument('--delete', '-d', action='store_true',
                        help=('Allows for deletion of objects and/or ' +
                            'containers'))
    parser.add_argument('--container', '-c', nargs='*',
                        help='Selects container(s) to delete contents from')
    parser.add_argument('--murder', action='store_true',
                        help=('Murder all objects and containers in an ' +
                            ' account'))
    parser.add_argument('--authtest', action='store_true',
                        help=('Tests auth every minute to cdn and api ' +
                            'endpoints FOREVER and outputs debug info if a ' +
                            '401 is encountered'))
    parser.add_argument('--cc', type=int, default=1,
                            help='Container delete concurrency')
    parser.add_argument('--oc', type=int, default=45,
                            help='Object delete concurrency')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--apikey', '-a',  help='Account api key')
    group.add_argument('--password', '-p', help='Account password')

    args = parser.parse_args()
    args.endpoint = args.endpoint.upper()
    if args.veryverbose:
        args.verbose = True
        print parser.parse_args()
    if (args.oc < 1) or (args.cc < 1):
        print 'Concurrency was less then 1, exiting.'
        sys.exit()
    if args.authtest:
        authtest()
    elif args.delete and not (args.murder or args.container):
        print ('You asked me to delete stuff ' +
            'without telling me what to remove.')
        sys.exit()
    else:
        global authdata
    if args.delete and args.murder:
        authdata = cfauth()
        get_containers()
    elif args.delete and args.container:
        try:
            authdata = cfauth()
            container_delete_pool = multiprocessing.Pool(args.cc)
            container_delete_pool.map(del_container, args.container)
            container_delete_pool.close()
            container_delete_pool.join()
        except (KeyboardInterrupt, SystemExit, Exception):
            container_delete_pool.terminate()
            raise
    else:
        cfauth()

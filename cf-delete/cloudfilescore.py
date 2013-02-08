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
Rackspace Cloud Files Shared Stuff
"""

import json
import httplib
from urllib2 import quote
import os

class container:
    """
    For container level operations
    """
    def put(self, container=None, args=None, authdata=None):
        endpoint = authdata['endpoint'].split('/')[2]
        connection = httplib.HTTPSConnection(endpoint, 443)
        headers = {'X-Auth-Token': authdata['token']}
        filepath = '/v1/' + authdata['tenantid'] + '/' + quote(container)
        while True:
            try:
                connection.request('PUT', filepath , '', headers)
                response = connection.getresponse()
                if args['verbose']:
                    #print response.status, response.reason
                    pass
                if args['veryverbose']:
                    print response.msg
                if response.status == 401:
                    authdata=auth.cfendpoint(args)
                    continue
                connection.close()
                break
            except httplib.BadStatusLine:
                pass


class object:
    """
    For object level operations
    """
    def put(self, container, obj, fullpath, args, authdata,
            metadata=None, connection=None):
        """
        Upload an object with metadata optionally specified

        requires the following to be defined:
        container - container you wish to upload to
        obj - object name
        fullpath - full path to file
        args - args that were passed into the calling script
        authdata - authdata, returned from the auth class
        """
        endpoint = authdata['endpoint'].split('/')[2]
        if not connection:
            connection = httplib.HTTPSConnection(endpoint, 443)
            streamlining = True
        else:
            streamlining = False
        headers = {'X-Auth-Token': authdata['token']}
        filepath = ('/v1/' + authdata['tenantid'] + '/' +
                    quote(container) + '/' + quote(obj, safe=''))
        while True:
            try:
                if os.path.isdir(fullpath):
                    headers['Content-Length']=0
                    connection.request('PUT', filepath, '', headers)
                    response = connection.getresponse()
                    response.read()
                else:
                    connection.request('PUT', filepath,
                                       open(fullpath, 'rb'), headers)
                    response = connection.getresponse()
                    response.read()
                if args['verbose']:
                    #print response.status, response.reason
                    pass
                if args['veryverbose']:
                    print response.msg
                if response.status == 401:
                    authdata=auth.cfendpoint(args)
                    continue
                if not streamlining:
                    connection.close()
                break
            except httplib.BadStatusLine:
                pass
            finally:
                pass


class auth:
    """
    Auth Superclass
    """
    def cfendpoint(self, args=None):
    #def cfauth(user=None, apikey=None, region=None):
        """
        Authenticate and return authentication details via returned dict
        """
        if args['endpoint'] == 'LON':
            authurl = 'lon.auth.api.rackspacecloud.com'
        else:
            authurl = 'auth.api.rackspacecloud.com'
        if args['password']:
            jsonreq = json.dumps({'auth': {'passwordCredentials': {
                                'username': args['apiuser'],
                                'password': args['password']}}})
        else:
            jsonreq = json.dumps({'auth': {'RAX-KSKEY:apiKeyCredentials':
                                {'username': args['apiuser'],
                                'apiKey': args['apikey']}}})
        if args['veryverbose']:
            print 'JSON REQUEST: ' + jsonreq

        #make the request
        connection = httplib.HTTPSConnection(authurl, 443)
        if args['veryverbose']:
            connection.set_debuglevel(1)
        headers = {'Content-type': 'application/json'}
        connection.request('POST', '/v2.0/tokens', jsonreq, headers)
        json_response = json.loads(connection.getresponse().read())
        connection.close()

        #process the request
        if args['veryverbose']:
            print 'JSON decoded and pretty'
            print json.dumps(json_response, indent=2)
        cfdetails = {}
        try:
            catalogs = json_response['access']['serviceCatalog']
            for service in catalogs:
                if service['name'] == 'cloudFiles':
                    for endpoint in service['endpoints']:
                        if endpoint['region'] == args['endpoint']:
                            if args['internal']:
                                cfdetails['endpoint'] = endpoint['internalURL']
                            else:
                                cfdetails['endpoint'] = endpoint['publicURL']
                            cfdetails['tenantid'] = endpoint['tenantId']
                elif service['name'] == 'cloudFilesCDN':
                    for endpoint in service['endpoints']:
                        if endpoint['region'] == args['endpoint']:
                            cfdetails['cdnendpoint'] = endpoint['publicURL']
            cfdetails['token'] = json_response['access']['token']['id']
            if args['verbose']:
                print 'CDN Endpoint:\t', cfdetails['cdnendpoint']
                print 'Endpoint:\t', cfdetails['endpoint']
                print 'Tenant:\t\t', cfdetails['tenantid']
                print 'Token:\t\t', cfdetails['token']
        except(KeyError, IndexError):
            print 'Error while getting answers from auth server.'
            print 'Check the endpoint and auth credentials.'
        return cfdetails
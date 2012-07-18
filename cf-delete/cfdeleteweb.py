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
Rackspace Cloud Files Deletion CGI Script
"""

import cgi
import os
import sys
import re
import json
import httplib


def check_if_running(apikey=None):
    """
    gets the process list, not including strings with grep in thier name
    """
    if apikey == None:
        print '\t\tNo API key entered, please enter one.'
        closebody()
    #get process list
    processes = [(int(p), c) for p, c in [x.rstrip('\n').split(' ', 1)
        for x in os.popen('ps h -eo pid:1,cfcommand | grep -v \'grep \' ' +
            '| grep ' + apikey)]]
    #stages for run or quit
    if processes:
        print '\t\tDelete process already running for this API key.'
        print '\t\tquiting'
        closebody()
    else:
        return False


def cfauth(user=None, apikey=None, region=None):
    """
    Authenticate and return authentication details via returned dict
    """
    if region:
        region = region.upper()
    if region == 'LON':
        authurl = 'lon.auth.api.rackspacecloud.com'
    else:
        authurl = 'auth.api.rackspacecloud.com'
    jsonreq = json.dumps({'auth': {'RAX-KSKEY:apiKeyCredentials':
        {'username': user, 'apiKey': apikey}}})

    #make the request
    connection = httplib.HTTPSConnection(authurl, 443)
    headers = {'Content-type': 'application/json'}
    connection.request('POST', '/v2.0/tokens', jsonreq, headers)
    json_response = json.loads(connection.getresponse().read())
    connection.close()
    #process the request
    cfdetails = {}
    try:
        catalogs = json_response['access']['serviceCatalog']
        for service in catalogs:
            if service['name'] == 'cloudFiles':
                for endpoint in service['endpoints']:
                    if endpoint['region'] == region:
                        cfdetails['endpoint'] = endpoint['publicURL']
                        cfdetails['tenantid'] = endpoint['tenantId']
                    if endpoint['region'] == 'ORD':
                        cfdetails['ORD'] = True
                        cfdetails['ORD-ENDPOINT'] = endpoint['publicURL']
                        cfdetails['tenantid'] = endpoint['tenantId']
                    else:
                        cfdetails['ORD'] = False
                    if endpoint['region'] == 'DFW':
                        cfdetails['DFW'] = True
                        cfdetails['DFW-ENDPOINT'] = endpoint['publicURL']
                        cfdetails['tenantid'] = endpoint['tenantId']
                    else:
                        cfdetails['DFW'] = False
                    if endpoint['region'] == 'LON':
                        cfdetails['LON'] = True
                        cfdetails['LON-ENDPOINT'] = endpoint['publicURL']
                        cfdetails['tenantid'] = endpoint['tenantId']
                    else:
                        cfdetails['LON'] = False
            elif service['name'] == 'cloudFilesCDN':
                for endpoint in service['endpoints']:
                    if endpoint['region'] == region:
                        cfdetails['cdnendpoint'] = endpoint['publicURL']
        cfdetails['token'] = json_response['access']['token']['id']
    except(KeyError, IndexError):
        print '\t\tError while getting answers from auth server.<br>'
        print '\t\tCheck the endpoint and auth credentials.<br>'
        print '\t\tExiting now, please try again.<br>'
        closebody()
    return cfdetails


def closebody():
    print '\t</body>'
    print '</html>'
    sys.exit()


def convert_bytes(bytes):
    bytes = float(bytes)
    if bytes >= 1099511627776:
        terabytes = bytes / 1099511627776
        size = '%.2fT' % terabytes
    elif bytes >= 1073741824:
        gigabytes = bytes / 1073741824
        size = '%.2fG' % gigabytes
    elif bytes >= 1048576:
        megabytes = bytes / 1048576
        size = '%.2fM' % megabytes
    elif bytes >= 1024:
        kilobytes = bytes / 1024
        size = '%.2fK' % kilobytes
    else:
        size = '%.2fb' % bytes
    return size


def check_cf_stats(user=None, apikey=None, region=None, authdata=None):
    """
    Prints out Cloud files statistics for a given account endpoint.
    """
    if region == None:
        print '\t\tNo region specified<br>'
        closebody()
    headers = {'X-Auth-Token': authdata['token']}
    endpoint = authdata['endpoint'].split('/')[2]
    connection = httplib.HTTPSConnection(endpoint, 443)
    connection.request('HEAD', '/v1/' + authdata['tenantid'], '', headers)
    response = connection.getresponse()
    response.read()
    connection.close()
    if response.status == 401:
        print '\t\tError retrieving Cloud Files details, please try again'
        closebody()
    #get Cloud Files metedata
    containercount = int(response.getheader('X-Account-Container-Count'))
    objectcount = int(response.getheader('X-Account-Object-Count'))
    storageused = int(response.getheader('X-Account-Bytes-Used'))
    storageused = convert_bytes(storageused)
    if containercount == 0 and objectcount == 0:
        print '\t\tNothing to delete in Files.<br>'
        closebody()
    #calculate approximate time til completion
    min_container_delete_time = containercount / 4.0
    max_container_delete_time = containercount / 3.0
    object_delete_time = objectcount / 90.0
    min_delete_time = (min_container_delete_time + object_delete_time)
    max_delete_time = (max_container_delete_time + object_delete_time)
    #get minimum time it will take
    if 60 < min_delete_time < 3600:
        min_delete_time_string = str(min_delete_time / 60.0) + ' minutes'
    elif 3600 < min_delete_time:
        min_delete_time_string = str(min_delete_time / 3600.0) + ' hours'
    else:
        min_delete_time_string = str(min_delete_time) + ' seconds'
    #get maximum time it will take
    if 60 < max_delete_time < 3600:
        max_delete_time_string = str(max_delete_time / 60.0) + ' minutes'
    elif 3600 < max_delete_time:
        max_delete_time_string = str(max_delete_time / 3600.0) + ' hours'
    else:
        max_delete_time_string = str(max_delete_time) + ' seconds'
    #print output
    print '\t\tContainer:', containercount, '<br>'
    print '\t\tObjects:', objectcount, '<br>'
    print '\t\tStorage Used:', storageused, '<br><br>'
    print ('\t\tThis should approximately between ' + min_delete_time_string +
        ' and ' + max_delete_time_string + ' to complete.<br>')
    print ('\t\tI\'d check this again (if the delete is already ' +
        'running) an hour or two after the delete is scheduled to ' +
        ' finish and possibly re-run if needed.<br>')
    closebody()


if __name__ == '__main__':
    """
    runs cfdelete from cgi
    """
    #get input
    global user
    global api
    global region
    form = cgi.FieldStorage()
    user = form.getvalue('username')
    api = form.getvalue('api_key')
    region = form.getvalue('region')
    submit = form.getvalue('submit')
    region = form.getvalue('ukaccount')
    #clean up input
    pattern = re.compile('\W')
    if user:
        user = re.sub(pattern, '', user)
    else:
        user = None
    if api:
        api = re.sub(pattern, '', api)
    else:
        api = None
    if region:
        region = re.sub(pattern, '', region)
        region = region.lower()
    else:
        region = None
    if submit:
        submit = re.sub(pattern, '', submit)
    else:
        submit = None
    cfcommand = ('/usr/bin/at now <<< \'/usr/bin/python ' +
        '/var/www/localhost/cgi-bin/cfdelete.py --murder -d --cc 2 ' +
        '-u ' + user + ' -a ' + api + ' -e ')
    cscommand = ('/usr/bin/at now <<< \'/usr/bin/python ' +
        '/var/www/localhost/cgi-bin/csdelete.py --murder' +
        '-u ' + user + ' -a ' + api + ' -e ')
    if region:
        cscommand = cscommand + region
    else:
        cscommand = cscommand + 'ord'
    #check if already running and if so, die
    print 'Content-type: text/html\n'
    print '<html>'
    print '\t<head>'
    print '\t\t<title>Results</title>'
    print '\t</head>'
    print '\t<body>'
    if submit == 'delete':
        if check_if_running(api) == False:
            authdata = cfauth(user, api, region)
            print ('\t\t<h1>You may have to run this again once it ' +
                'completes but should not have to (hopefully).</h1><br>')
            print ('\t\t<h1>This is an estemate of how long it will take ' +
                'only.</h1>')
            print ('\t\t<h2>To be safe, I generall add about 10-25% to the ' +
                'time it will take to delete.</h2>')
            if authdata['ORD']:
                print ('\t\t<br><br><br><h3>Endpoint in ORD detected, ' +
                    'here is what should be deleted and ' +
                    'about how long it should take:</h3>')
                cfcommand = cfcommand + 'ord\''
                os.system(cfcommand)
                check_cf_stats(user, api, 'ord', authdata)
            elif authdata['DFW']:
                print ('\t\t<br><br><br><h3>Endpoint in DFW detected, ' +
                    'here is what should be deleted and ' +
                    'about how long it should take:</h3>')
                cfcommand = cfcommand + 'dfw\''
                os.system(cfcommand)
                check_cf_stats(user, api, 'dfw', authdata)
            elif authdata['LON']:
                print ('\t\t<br><br><br><h3>Endpoint in LON detected, ' +
                    'here is what should be deleted and ' +
                    'about how long it should take:</h3>')
                cfcommand = cfcommand + 'lon\''
                os.system(cfcommand)
                check_cf_stats(user, api, 'lon', authdata)
            os.system(cscommand)
    elif submit == 'check_cf_stats':
        print ('\t\t<h1>This is an estemate of how long it will take only' +
            '.</h1>')
        print ('\t\t<h2>To be safe, I generall add about 10-25% to the ' +
            'time it will take to delete.</h2>')
        authdata = cfauth(user, api, region)
        if authdata['ORD']:
            region = 'ord'
            authdata['endpoint'] = authdata['ORD-ENDPOINT']
            print ('\t\t<br><br><br><h3>Endpoint in ORD detected, ' +
                    'here is what should be deleted and ' +
                    'about how long it should take:</h3>')
            check_cf_stats(user, api, region, authdata)
        if authdata['DFW']:
            region = 'dfw'
            authdata['endpoint'] = authdata['DFW-ENDPOINT']
            print ('\t\t<br><br><br><h3>Endpoint in DFW detected, ' +
                    'here is what should be deleted and ' +
                    'about how long it should take:</h3>')
            check_cf_stats(user, api, region, authdata)
        if authdata['LON']:
            region = 'lon'
            authdata['endpoint'] = authdata['LON-ENDPOINT']
            print ('\t\t<br><br><br><h3>Endpoint in LON detected, ' +
                    'here is what should be deleted and ' +
                    'about how long it should take:</h3>')
            check_cf_stats(user, api, region, authdata)

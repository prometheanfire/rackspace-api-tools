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
Rackspace Cloud Files Bulk Import Script
"""

import argparse
import os, re
from cloudfilescore import auth, container, object
from multiprocessing import freeze_support, JoinableQueue, Process
from multiprocessing.queues import Empty


def object_consumer(container_name, args, authdata,
                    queue, metadata=None):
    """
    Uploads objects in a to a container
    """
    import httplib
    while True:
        try:
            paths = queue.get(True, 10)
            #stops if the end marker is there (set to None)
            if not paths:
                break
            endpoint = authdata['endpoint'].split('/')[2]
            conn = httplib.HTTPSConnection(endpoint, 443)
            if args['veryverbose']:
                conn.set_debuglevel(1)
            for path in paths:
                #123//opt/oracle-jre-bin-1.7.0.13/lib/desktop/icons/LowContrast/48x48/apps/sun-javaws.png
                if not args['container']:
                    obj = re.sub('^' + re.escape(args['dir']) +
                                container_name + '/', '', path, count=1)
                else:
                    obj = re.sub('^' + re.escape(args['dir']), '', path, count=1)
                object().put(container_name, obj, path, args,
                             authdata, connection=conn)
            conn.close()
        except Empty:
            print 'Nothing to process in object queue, quiting'
            break
        finally:
            queue.task_done()


def container_consumer(args=None, authdata=None, queue=None):
    """
    Separates Jobs into sub-jobs based on objects up to 100 objects per second.
    """
    while True:
        try:
            #container initialization
            container_name = queue.get(True, 10)
            #stops if the end marker is there (set to None)
            if not container_name:
                break
            paths = []
            container().put(container_name, args, authdata)
            #object enumeration
            if not args['container']:
                container_dir = os.path.join(args['dir'], container_name) + '/'
            else:
                container_dir = os.path.join(args['dir']) + '/'
            for dir_path, dir_names, file_names in os.walk(container_dir):
                for file in file_names:
                    paths.append(os.path.abspath(os.path.join(dir_path, file)))
                for dir in dir_names:
                    if not os.listdir(os.path.abspath(os.path.join(dir_path, dir))):
                        paths.append(os.path.abspath(os.path.join(dir_path, dir)))
            if paths:
                #setup object queues
                object_queue = JoinableQueue()
                for object_worker in range(args['oc']):
                    job = Process(target=object_consumer,
                                  args=(container_name, args,
                                        authdata, object_queue,))
                    job.daemon=True
                    job.start()
                paths_list = ([paths[i:i + args['oc']]
                                      for i in range(0, len(paths), args['oc'])])
                for sublist in paths_list:
                    object_queue.put(sublist)
                #tailing the queue with a Null marker
                #so the workers shut down nicely.
                for object_worker in range(args['oc']):
                    object_queue.put(None)
                object_queue.join()
        except Empty:
            print 'Nothing to process in container queue, quiting'
            break
        finally:
            queue.task_done()


def upload(args=None, authdata=None):
    """
    Initialize the containers and pseudo-directories for what is to be
    uploaded.  Separates jobs into sub-jobs based on container.
    Up to 100 containers per second.
    """
    #initalize the containers in parallel
    containers = []
    for obj in os.listdir(args['dir']):
        if args['container']:
            containers.append(args['container'])
            break
        #if os.path.isdir(os.path.abspath(args['dir']+'/'+obj)):
        if os.path.isdir(os.path.join(args['dir'], obj)):
            containers.append(obj)
    if containers:
        #set container job count to the less of args['cc'] or container count
        if args['cc'] < len(containers):
            args['cc'] = len(containers)
        #create queue and jobs
        container_queue = JoinableQueue()
        for container_worker in range(args['cc']):
            job = Process(target=container_consumer,
                          args=(args, authdata, container_queue,))
            job.daemon=False
            job.start()
        for container in containers:
            container_queue.put(container)
        #tailing the queue with a Null marker so the works shut down nicely.
        for container in range(args['cc']):
            container_queue.put(None)
        container_queue.join()

if __name__ == '__main__':
    freeze_support()
    parser = argparse.ArgumentParser(description='Command line options')
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
    parser.add_argument('--download', '-d', action='store_true',
                        help='Downloads the account to local storage')
    parser.add_argument('--container', '-c',
                        help='Selects container to upload to ' +
                             'instead of multiple containers at an endpoint)')
    parser.add_argument('--dir', required=True,
                        help='The source directory you wish to sync')
    parser.add_argument('--cc', type=int, default=3, choices=range(1,101),
                            help='Container concurrency')
    parser.add_argument('--oc', type=int, default=10, choices=range(1,101),
                            help='Object concurrency')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--apikey', '-a',  help='Account api key')
    group.add_argument('--password', '-p', help='Account password')

    args = parser.parse_args()
    args.endpoint = args.endpoint.upper()
    if args.veryverbose:
        args.verbose = True
        print parser.parse_args()
    #redefining args into a dict
    dict_args = vars(args)
    authdata = auth().cfendpoint(dict_args)
    upload(dict_args, authdata)

import json
from pprint import pformat
import re
import os
from uuid import UUID

import argparse
from tarjan import tarjan

parser = argparse.ArgumentParser(description='debug failed consensus test')
parser.add_argument('path', help='path to the debug info')
parser.add_argument('key', help='key to examine')
parser.add_argument('--replica',  '-r', metavar='replica', type=int, default=0, help='replica to work with')
parser.add_argument('--instance',  '-i', metavar='instance', default=None, help='initial instance')
argv = parser.parse_args()

REPLICA = argv.replica
PATH = re.sub(r'\/+$', '', argv.path)
KEY = argv.key

print ""
print ""

if not os.path.isdir(PATH):
    print "invalid path", PATH

if not any([fname.startswith('{}.'.format(KEY)) for fname in os.listdir(PATH)]):
    print "invalid key", KEY

PREACCEPTED = 1
ACCEPTED = 2
COMMITTED = 3
EXECUTED = 4
instances = {}
execution_order = []


class Instance(object):

    def __init__(self, data):
        self._data = data
        self.iid = data['InstanceID']
        self.leader_id = data['LeaderID']
        self.successors = data['Successors']
        self.command = data['Command']
        self.status = data['Status']
        self.seq = data['Sequence']
        self._deps = set(data['Dependencies'])
        self.strongly_connected = set(data.get('StronglyConnected'))
        self._in_deps = set()

        self.deps_log = data.get('DepsLog')

    def __repr__(self):
        return pformat(self._data)

    def _add_in_dep(self, iid):
        self._in_deps.add(iid)

    @classmethod
    def _add(cls, data):
        instance = cls(data)
        instances[instance.iid] = instance
        return instance

    @property
    def deps(self):
        return [instances.get(d) for d in self._deps]

    @property
    def in_deps(self):
        return [instances.get(d) for d in self._in_deps]

    def depends_on(self, *iids):
        return [iid in self._deps for iid in iids]

    def is_dependency_of(self, *iids):
        return [iid in self._in_deps for iid in iids]

    @property
    def is_strongly_connected(self):
        return len(self.strongly_connected) > 0


with open('{}/{}:{}.instances.json'.format(PATH, KEY, REPLICA)) as f:
    for data in json.load(f):
        Instance._add(data)

    for instance in instances.values():
        for dep in instance._deps:
            dep_instance = instances.get(dep)
            if dep_instance is not None:
                dep_instance._add_in_dep(instance.iid)

with open('{}/{}:{}.execution.json'.format(PATH, KEY, REPLICA)) as f:
    for data in json.load(f):
        execution_order.append(data['InstanceID'])

last_executed = instances.get(execution_order[-1])

# work out the expected execution order
dep_graph = {}
for instance in instances.values():
    dep_graph[instance.iid] = instance._deps

tsorted = tarjan(dep_graph)


def _component_cmp(x, y):
    x = instances[x]
    y = instances[y]
    xID = UUID(x.iid)
    yID = UUID(y.iid)
    if x.seq != y.seq:
        return int(x.seq - y.seq)
    elif xID.time != yID.time:
        return int(xID.time - yID.time)
    else:
        return -1 if xID.bytes < yID.bytes else 1

strong_map = {}
for component in tsorted:
    if len(component) > 1:
        cset = set(component)
        for iid in cset:
            strong_map[iid] = cset


expected_execution_order = sum([sorted(c, cmp=_component_cmp) for c in tsorted], [])

minlen = min(len(execution_order), len(expected_execution_order))

def check_consistency(history=5):
    for i, (actual, expected) in enumerate(zip(execution_order[:minlen], expected_execution_order[:minlen])):
        if actual != expected:
            print "execution inconsistency at", i
            print ""
            start = max(0, i-history)
            end = i + history
            print '{:<36} {:<36}'.format('actual', 'expected')
            for a, e in zip(execution_order[start:end], expected_execution_order[start:end]):
                print a, e, '<-' if a != e else ''

            break

check_consistency()

print ""
print ""

def check_strong_connections():
    for iid, component in strong_map.items():
        instance = instances[iid]
        if instance.strongly_connected != component:
            if instance.status >= COMMITTED:
                print "strongly connected inconsistency for", iid
                print "  expected:", component
                print "       got:", instances[iid].strongly_connected
                print ""

check_strong_connections()

instance = instances.get(argv.instance)
if instance:
    print str(instance)

i = instances
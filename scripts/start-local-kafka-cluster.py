#!/usr/bin/env python3

import signal
import sys
import subprocess
import re
import copy
import json
import argparse
import os
import shutil
import glob
import time
import uuid
from multiprocessing import Process
from string import Template
from collections import namedtuple

KAFKA_SERVER_START_BIN = 'kafka-server-start.sh'


################################################################################

controllerPids = []
brokerPids = []

class ProcessPool(object):
    def __init__(self):
        self.processList = []

    def addProcess(self, cmd, name, outFile, errFile):
        out = open(outFile, 'w')
        err = open(errFile, 'w')
        p = subprocess.Popen(cmd,
                             shell=True,
                             stdin=subprocess.PIPE,
                             stdout=out,
                             stderr=err)

        if 'controller' in name:
            controllerPids.append(p.pid)
        elif 'broker' in name:
            brokerPids.append(p.pid)

        self.processList.append((p, name))

    def run(self):
        anyFailure = False
        while self.processList:
            for (i, (p, name)) in enumerate(self.processList):
                ret = p.poll()
                if ret != None:
                    print('Failed to start server: {0}, pid: {1}, ret: {2}'.format(name, p.pid, ret))
                    self.processList.pop(i)
                    anyFailure = True
                    break
        if anyFailure:
            self.terminate()

    def terminate(self):
        for (p, name) in self.processList:
            p.kill()
            print('{0} terminated'.format(name))

    def __del__(self):
        self.terminate()

processPool=ProcessPool()



def executeCommand(cmd):
    cmdResult = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if cmdResult.returncode == 0:
        print(f"[OK] {cmd}")
        return cmdResult.stdout
    else:
        print(f"[Failed] {cmd}")
        print(cmdResult.stderr)


################################################################################

def generateControllerConfig(nodeId, port, logDir):
    return f'''
        process.roles=controller
        node.id={nodeId}
        controller.quorum.bootstrap.servers=localhost:{port}
        listeners=CONTROLLER://:{port}
        advertised.listeners=CONTROLLER://localhost:{port}
        controller.listener.names=CONTROLLER
        num.network.threads=3
        num.io.threads=8
        socket.send.buffer.bytes=102400
        socket.receive.buffer.bytes=102400
        socket.request.max.bytes=104857600
        log.dirs={logDir}
        num.partitions=1
        num.recovery.threads.per.data.dir=2
        offsets.topic.replication.factor=3
        min.insync.replicas=2
        transaction.state.log.replication.factor=3
        transaction.state.log.min.isr=2
        share.coordinator.state.topic.replication.factor=3
        share.coordinator.state.topic.min.isr=2
        log.retention.hours=168
        log.segment.bytes=1073741824
        log.retention.check.interval.ms=300000
    '''


def generateBrokerConfig(nodeId, port, controllerPort, logDir):
    return f'''
        process.roles=broker
        node.id={nodeId}
        controller.quorum.bootstrap.servers=localhost:{controllerPort}
        listeners=PLAINTEXT://localhost:{port}
        inter.broker.listener.name=PLAINTEXT
        advertised.listeners=PLAINTEXT://localhost:{port}
        controller.listener.names=CONTROLLER
        listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
        num.network.threads=3
        num.io.threads=8
        socket.send.buffer.bytes=102400
        socket.receive.buffer.bytes=102400
        socket.request.max.bytes=104857600
        log.dirs={logDir}
        num.partitions=1
        num.recovery.threads.per.data.dir=2
        offsets.topic.replication.factor=3
        min.insync.replicas=2
        transaction.state.log.replication.factor=3
        transaction.state.log.min.isr=2
        share.coordinator.state.topic.replication.factor=3
        share.coordinator.state.topic.min.isr=2
        log.retention.hours=168
        log.segment.bytes=1073741824
        log.retention.check.interval.ms=300000
        auto.create.topics.enable=false
    '''

################################################################################

def startServer(name, propFile, outDir):
    cmd = '{0} {1}'.format(KAFKA_SERVER_START_BIN, propFile)
    processPool.addProcess(cmd, name, os.path.join(outDir, name+'.out'), os.path.join(outDir, name+'.err'))

################################################################################

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--controller-port', help='The port for Kafka controller', required=True)
    parser.add_argument('--broker-ports', nargs='+', help='The ports for Kafka brokers', required=True)
    parser.add_argument('--temp-dir', help='The location for log files, printout, etc', default=f"{os.path.join(os.getcwd(), 'tmp')}")
    parsed = parser.parse_args()

    controllerPort = parsed.controller_port
    brokerPorts   = parsed.broker_ports

    if os.path.exists(parsed.temp_dir):
        shutil.rmtree(parsed.temp_dir)

    logDir  = os.path.join(parsed.temp_dir, 'log')
    outDir  = os.path.join(parsed.temp_dir, 'out')
    propDir = os.path.join(parsed.temp_dir, 'properties')
    os.makedirs(logDir)
    os.makedirs(outDir)
    os.makedirs(propDir)

    PropFile = namedtuple('PropertiesFile', 'filename context')

    # Generate properties files
    controllerPropFiles = []
    controllerLogDir = os.path.join(logDir, 'controller')
    controllerPropFiles.append(PropFile(os.path.join(propDir, 'controller.properties'), generateControllerConfig(10, controllerPort, controllerLogDir)))

    executeCommand(f"mkdir {controllerLogDir}")

    brokerPropFiles = []
    for (i, brokerPort) in enumerate(brokerPorts):
        nodeId = i+1
        brokerPropFiles.append(PropFile(os.path.join(propDir, f"broker{nodeId}.properties"), generateBrokerConfig(nodeId, brokerPort, controllerPort, os.path.join(logDir, f"broker{nodeId}"))))

    randomUuid = str(uuid.uuid4())

    for propFile in (set(controllerPropFiles) | set(brokerPropFiles)):
        with open(propFile.filename, 'w') as f:
            f.write(propFile.context)

        executeCommand(f"kafka-storage.sh format {'--standalone' if 'controller' in propFile.filename else ''} -t {randomUuid} -c {propFile.filename}")


    startServer('controller', controllerPropFiles[0].filename, outDir)

    time.sleep(5)

    for (i, brokerPort) in enumerate(brokerPorts):
        startServer('broker{0}'.format(i), brokerPropFiles[i].filename, outDir)

    time.sleep(5)
    MAX_RETRY = 60
    retry = 0
    while retry < MAX_RETRY:
        time.sleep(1)
        kafkaBrokerPids = []

        for brokerPort in brokerPorts:
            if output := executeCommand(f"lsof -nP -iTCP:{brokerPort} | grep LISTEN"):
                matched = re.search(r'[^\s-]+ +([0-9]+) +.*', output)
                if matched:
                    kafkaBrokerPids.append(matched.group(1))

        print(f"brokerpids: {kafkaBrokerPids}")

        if len(kafkaBrokerPids) != len(brokerPorts):
            retry += 1
            continue

        with open(r'test.env', 'w') as envFile:
            envStr = f"export KAFKA_BROKER_LIST={','.join(['127.0.0.1:{0}'.format(port) for port in brokerPorts])}\n"
            envStr += f"export KAFKA_BROKER_PIDS={','.join([pid for pid in kafkaBrokerPids])}\n"
            print(f"Output to test.env:\n{envStr}\n")
            envFile.write(envStr)
            break

    if retry < MAX_RETRY:
        print('Kafka cluster started with ports: {0}!'.format(brokerPorts))
        processPool.run()
    else:
        print('Kafka cluster failed to start with ports: {0}!'.format(brokerPorts))
        processPool.terminate()
        for filename in glob.glob(os.path.join(outDir, '*')):
            with open(filename, 'r') as f:
                print('^^^^^^^^^^ {0} ^^^^^^^^^^'.format(os.path.basename(filename)))
                print(f.read())
                print('vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv')

if __name__ == '__main__':
    main()


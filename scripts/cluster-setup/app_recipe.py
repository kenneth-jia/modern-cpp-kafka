import yaml
import os
import getpass
import copy

import subprocess
from string import Template

import platform

from pathlib import Path



class AppRecipe(object):
    def __init__(self, recipeFilename):
        self.sourcePath = Path(__file__).parent.absolute()

        self.env = {}
        self.appTemplates = {}
        self.applications = []

        with open(recipeFilename, 'r') as stream:
            self.recipe = yaml.safe_load(stream)

        self.resolveGlobalEnv()
        self.parseAppTemplates()
        self.parseApplications()

    def outputScriptsToFile(self, commands, outputFilename):
        os.makedirs(Path(outputFilename).parent, exist_ok=True)

        with open(outputFilename, 'w+') as outputFile:
            outputFile.writelines(['{0}\n'.format(cmd) for cmd in commands])
            os.chmod(outputFilename, 0o755)
            print(f'    Output: {outputFilename}')

 
    def outputScriptsFiles(self):
        for application in self.applications:
            print('Generating scripts for application: {0} ...'.format(application['name']))
            self.outputEnvFile(application)
            self.outputConfigFiles(application)
            self.outputAppStartCmdFiles(application)

        print('Generating scripts to start all applications...')
        self.outputStartAllAppCmdFile()


    def outputEnvFile(self, application):
        appName = application['name']
        env = application['env']

        outputDir = os.path.join(self.env['ROOT_OUTPUT_PATH'], appName)
        envFilename = os.path.join(outputDir, 'envfile')

        commands = ['export {0}="{1}"'.format(k, env[k]) for k in env]
        self.outputScriptsToFile(commands, envFilename)


    def outputConfigFiles(self, application):
        appName = application['name']
        configFilesMap = application['config-files']

        outputDir = os.path.join(self.env['ROOT_OUTPUT_PATH'], appName)

        for filename in configFilesMap:
            configFilename = os.path.join(outputDir, filename)
            envFilename = os.path.join(outputDir, 'envfile')
            templateFilename = os.path.join(self.sourcePath, 'config-file-templates', configFilesMap[filename])
            os.system('source {0} && envsubst < {1} > {2}'.format(envFilename, templateFilename, configFilename))
            print(f'    Output: {envFilename}')

    def outputAppStartCmdFiles(self, application):
        appName = application['name']
        outputDir = os.path.join(self.env['ROOT_OUTPUT_PATH'], appName)
        appStartCmdFilename = os.path.join(outputDir, 'app-start-cmd.bash')
        appStartCmd = application['app-start-cmd']
        additionalBinPath = self.env['ADDITIONAL_BIN_PATH']
        scriptsDir = os.path.join(self.sourcePath, 'scripts')
        envFilename = os.path.join(self.env['ROOT_OUTPUT_PATH'], appName, 'envfile')       

        commands = ['#!/usr/bin/env bash',
                    f'export PATH={additionalBinPath}:{scriptsDir}:$PATH',
                    f'source {envFilename}',
                    f'cd {outputDir}',
                    f'{appStartCmd}']

        self.outputScriptsToFile(commands, appStartCmdFilename)

    def outputStartAllAppCmdFile(self):
        commands = []
        for application in self.applications:
            appName = application['name']
            commands.append(os.path.join(self.env['ROOT_OUTPUT_PATH'], appName, 'app-start-cmd.bash') + '&')

        startAllAppCmdFilename = os.path.join(self.env['ROOT_OUTPUT_PATH'], 'start-all-app-cmd.sh')
        self.outputScriptsToFile(commands, startAllAppCmdFilename)


    def resolveGlobalEnv(self):
        self.env['HOSTNAME'] = platform.node()
        self.env['USER']     = getpass.getuser()
        globalEnv = self.recipe['global-environments']
        if globalEnv:
            self.env['ADDITIONAL_BIN_PATH']  = globalEnv['ADDITIONAL_BIN_PATH']
            self.env['ROOT_OUTPUT_PATH'] = globalEnv['ROOT_OUTPUT_PATH']


    def parseAppTemplates(self):
        templates = self.recipe['application-templates']
        for templateName in templates:
            self.appTemplates[templateName] = {}
            self.appTemplates[templateName]['config-files'] = copy.deepcopy(templates[templateName]['config-files'])
            self.appTemplates[templateName]['env'] = copy.deepcopy(templates[templateName]['env'])
            self.appTemplates[templateName]['app-start-cmd'] = copy.deepcopy(templates[templateName]['app-start-cmd'])


    def parseApplications(self):
        appsRecipe = self.recipe['applications']
        for appRecipe in appsRecipe:
            appName = appRecipe['name']

            if (appRecipe['run-on-host'] != self.env['HOSTNAME'] and appRecipe['run-on-host'] != 'localhost'):
                print(f'{appName} skipped in the host.')
                continue

            print(f'Parsing scripts for application: {appName}...')

            app = {'name': appName}
            
            templateName = appRecipe['template']
            appTemplate = copy.deepcopy(self.appTemplates[templateName])

            app['env'] = copy.deepcopy(appRecipe['env'])
            app['env'].update(self.env)

            app['config-files'] = appTemplate['config-files']

            for envName in appTemplate['env']:
                envTemplate = Template(appTemplate['env'][envName])
                app['env'][envName] = envTemplate.substitute(app['env'])

            app['app-start-cmd'] = Template(appTemplate['app-start-cmd']).substitute(app['env'])

            self.applications.append(app)

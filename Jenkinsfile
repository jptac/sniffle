#!/usr/bin/env groovy
def labels = ["smartos_15_4_1"]
def builders = [:]


for (x in labels) {
    def label = x // Need to bind the label variable before the closure - can't do 'for (label in labels)'

    // Create a map to pass in to the 'parallel' step so we can fire all the builds at once
    builders[label] = {
      node(label) {
        // clean our workspace
        deleteDir()
        //setup env
        environment { 
            DSLABEL = label 
        }
        sh '''
        	env
        '''
        // checkout
        checkout scm
        GIT_BRANCH = sh(returnStdout: true, script: 'git rev-parse --abbrev-ref HEAD').trim()

        //build
        if (GIT_BRANCH == 'origin/master'){
        	sh '''
        		export PORTABLE=1
				export TERM=dumb
				export GPG_KEY=BB975564
				/opt/local/bin/make package
			'''
        } else {
        	sh '''
        		export PORTABLE=1
				export TERM=dumb
				export GPG_KEY=BB975564
				export SUFFIX=$(/opt/local/bin/erl -noshell -eval '{{Y, MM, D}, {H, M, S}} = calendar:universal_time(), io:format("pre~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B", [Y, MM, D, H, M, S]),init:stop()'); 
				/opt/local/bin/make package 
			'''
        }

        //create info file
        sh '''
        	mkdir -p rel/pkg/artifacts
        	cp rel/pkg/*.tgz rel/pkg/artifacts
			mkdir -p rel/pkg/info
			pkg_info -X rel/pkg/*.tgz > rel/pkg/info/$(pkg_info -X rel/pkg/*.tgz | awk -F "=" '/FILE_NAME/ {print $2}')

        '''

        //upload
        withAWS(region:'us-east-2', credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
        	s3Upload(file:'rel/pkg/artifacts/', bucket:'release-test.project-fifo.net', path:'pkg/15.4.1/dev/')
    		// do something
		}
      }
    }
}

parallel builders


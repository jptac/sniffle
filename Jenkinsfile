#!/usr/bin/env groovy
def labels = ["smartos_15_4_1", "smartos_16_4_0"]
def builders = [:]


for (x in labels) {
    def label = x // Need to bind the label variable before the closure - can't do 'for (label in labels)'

    // Create a map to pass in to the 'parallel' step so we can fire all the builds at once
    builders[label] = {
      node(label) {
        // build steps that should happen on all nodes go here
        checkout scm
        GIT_BRANCH = sh(returnStdout: true, script: 'git rev-parse --abbrev-ref HEAD').trim()

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

        sh '''
			mkdir rel/pkg/info
			pkg_info -X rel/pkg/*.tgz > rel/pkg/info/$(pkg_info -X rel/pkg/*.tgz | awk -F "=" '/FILE_NAME/ {print $2}')
        '''
        withAWS(profile:'Fifo PKG') {
        	s3Upload(file:'rel/pkg/*.tgz', bucket:'release-test.project-fifo.net', path:'pkg/15.4.1/dev')
    		// do something
		}
      }
    }
}

parallel builders


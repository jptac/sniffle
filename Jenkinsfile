#!/usr/bin/env groovy
//GIT_BRANCH = sh(returnStdout: true, script: 'git rev-parse --abbrev-ref HEAD').trim()
def labels = ["smartos_15_4_1", "smartos_16_4_0"]
def builders = [:]

for (x in labels) {
    def label = x // Need to bind the label variable before the closure - can't do 'for (label in labels)'

    // Create a map to pass in to the 'parallel' step so we can fire all the builds at once
    builders[label] = {
      node(label) {
        // build steps that should happen on all nodes go here
        sh """
        	export PORTABLE=1
			export TERM=dumb
			export GPG_KEY=BB975564

			#Comment out this line for REL 
			export SUFFIX=$(/opt/local/bin/erl -noshell -eval '{{Y, MM, D}, {H, M, S}} = calendar:universal_time(), io:format("pre~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B", [Y, MM, D, H, M, S]),init:stop()'); 

/opt/local/bin/make package
        """
      }
    }
}

parallel builders


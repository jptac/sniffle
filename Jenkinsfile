#!/usr/bin/env groovy
def labels = ["smartos_dataset_15.4.1"]
def builders = [:]


for (x in labels) {
    def label = x // Need to bind the label variable before the closure - can't do 'for (label in labels)'

    // Create a map to pass in to the 'parallel' step so we can fire all the builds at once
    builders[label] = {
      node(label) {
      	"Cleanup" : {
        	deleteDir()
    	}
        "Checkout" : {
        	checkout scm
        	BRANCH = sh(returnStdout: true, script: 'git rev-parse --abbrev-ref HEAD').trim()
    	}
        "Build" : {
        	build(BRANCH)
        }

        "Upload" : {
			//find ds version
			def matcher = env.NODE_LABELS =~ 'smartos_dataset_([^ ]+)'
	    	DS_VERSION = matcher[0][1];
	    	matcher = null
	    	
	        withAWS(region:'us-east-2', credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
	        	s3Upload(file:'rel/pkg/artifacts/', bucket:'release-test.project-fifo.net', path:"pkg/${DS_VERSION}/dev/")
			}
        }

      }
    }
}

stage("Fan Out") {
    steps {
		parallel builders
	}
}


def build (String git_branch) {
    SUFFIX = ""
    if (git_branch != 'origin/master'){
    	SUFFIX = '''
    		export SUFFIX=$(/opt/local/bin/erl -noshell -eval '{{Y, MM, D}, {H, M, S}} = calendar:universal_time(), io:format("pre~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B", [Y, MM, D, H, M, S]),init:stop()');
    	'''
    }

    def EXEC ="""
		export PORTABLE=1
		export TERM=dumb
		export GPG_KEY=BB975564
		${SUFFIX}
		/opt/local/bin/make package

		mkdir -p rel/pkg/artifacts
		mkdir -p rel/pkg/info
        cp rel/pkg/*.tgz rel/pkg/artifacts
        pkg_info -X rel/pkg/*.tgz > rel/pkg/info/$(pkg_info -X rel/pkg/*.tgz | awk -F "=" '/FILE_NAME/ {print $2}')
	"""

	sh EXEC
}

def ircSuccess(String ){
	sh ''' 
        MSG='This is the message here'
        SERVER=irc.freenode.net
        CHANNEL=#project-fifo
        USER=fifo_build_bot
    
        (
        echo NICK $USER
        echo USER $USER 8 * : $USER
        sleep 1
        #echo PASS $USER:$MYPASSWORD                                                                                                                                                       
        echo "JOIN $CHANNEL"
        echo "PRIVMSG $CHANNEL" :$MSG
        echo QUIT
        ) | nc $SERVER 6667
        
    '''
}
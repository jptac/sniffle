#!/usr/bin/env groovy
def labels = ["smartos_dataset_15.4.1"]
def builders = [:]


for (x in labels) {
    def label = x // Need to bind the label variable before the closure - can't do 'for (label in labels)'

    // Create a map to pass in to the 'parallel' step so we can fire all the builds at once
    builders[label] = {
      node(label) {
        // clean our workspace
      /*
        deleteDir()
        // checkout
        checkout scm
        BRANCH = sh(returnStdout: true, script: 'git rev-parse --abbrev-ref HEAD').trim()

        //build
        build(BRANCH)

        //create info file
        sh '''
        	mkdir -p rel/pkg/artifacts
        	cp rel/pkg/*.tgz rel/pkg/artifacts
			mkdir -p rel/pkg/info
			pkg_info -X rel/pkg/*.tgz > rel/pkg/info/$(pkg_info -X rel/pkg/*.tgz | awk -F "=" '/FILE_NAME/ {print $2}')

        '''
*/
        //find ds version
    //     def matcher = readFile('pom.xml') =~ '<version>(.+)</version>'
	//	  if (matcher) {
	//	    echo "Building version ${matcher[0][1]}"
	//	  }
    //   Pattern pattern = Pattern.compile("smartos_dataset_([^ ]+)");
		def matcher = env.NODE_LABELS =~ 'smartos_dataset_([^ ]+)'
    	def DS_VERSION = matcher[0][1];

      //  def DS_VERSION = Pattern.compile(/smartos_dataset_([^ ]+)/).matcher(env.NODE_LABELS).find()

        //sh(returnStdout: true, script: 'echo $NODE_LABELS | sed -n \'s/^.*\\(smartos_dataset_[^ ]*\\).*/\\1/p\' | awk -F\'_\' \'{print $3"}\'').trim()

        //upload
        
        withAWS(region:'us-east-2', credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
        	s3Upload(file:'rel/pkg/artifacts/', bucket:'release-test.project-fifo.net', path:"pkg/${DS_VERSION}/dev/")
    		// do something
		}
      }
    }
}

parallel builders


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
	"""

	sh EXEC
}

def upload = {

}

def notify = {

}

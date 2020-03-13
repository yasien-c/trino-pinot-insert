pipeline {
  agent any

  environment {
    MAJOR_VERSION = "330"
    GIT_SHA = sh(returnStdout: true, script: "git rev-parse --short=6 HEAD").trim()
    VERSION = "${GIT_SHA}-${MAJOR_VERSION}"
  }

  stages {
    stage("Annotate jenkins job") {
      steps {
        script {
          serviceName = params.REPO_URI.split("/")[-1].replace(".git", "")
          currentBuild.description = "${serviceName} - ${params.TAG_OR_SHA} : ${VERSION}"
        }
      }
    }

    stage("Build Presto Deployer Image") {
      environment {
        IMAGE_VERSION = "${VERSION}"
      }

      steps {
        sh """docker build . -f docker/presto-deploy/Dockerfile \
              -t docker.artifactory.cloudkitchens.internal/presto-deploy:${IMAGE_VERSION}
           """

        withDockerRegistry([credentialsId: "artifactory-jenkins-credentials", url: "https://docker.artifactory.cloudkitchens.internal"]) {
          sh "docker push docker.artifactory.cloudkitchens.internal/presto-deploy:${IMAGE_VERSION}"
        }
      }
    }

    stage("Build Presto Image") {
      environment {
        IMAGE_VERSION = "${VERSION}"
        PRESTO_VERSION = "${MAJOR_VERSION}"
        HUDI_BUCKET = "css-data-warehouse-hudi-lib"
        HUDI_DIR = "jar"
        HUDI_VERSION = "0.5.0-SNAPSHOT"
        HUDI_TAG = "6afb9783a6bb"
        JMX_EXPORTER_VERSION = "0.12.0"
        KEY_FILE = credentials("css-data-jenkins")
      }

      steps {
        sh "cp $KEY_FILE key.json"
        sh """docker build . -f docker/presto/Dockerfile \
                    --build-arg PRESTO_VERSION=$PRESTO_VERSION \
                    --build-arg JMX_EXPORTER_VERSION=${JMX_EXPORTER_VERSION} \
                    --build-arg KEY_FILE=key.json \
                    --build-arg HUDI_BUCKET=${HUDI_BUCKET} \
                    --build-arg HUDI_DIR=${HUDI_DIR} \
                    --build-arg HUDI_VERSION=${HUDI_VERSION} \
                    --build-arg HUDI_TAG=${HUDI_TAG} \
                    -t docker.artifactory.cloudkitchens.internal/presto:${IMAGE_VERSION}
                   """
        withDockerRegistry([credentialsId: "artifactory-jenkins-credentials", url: "https://docker.artifactory.cloudkitchens.internal"]) {
          sh "docker push docker.artifactory.cloudkitchens.internal/presto:${IMAGE_VERSION}"
        }
      }
    }
  }
}

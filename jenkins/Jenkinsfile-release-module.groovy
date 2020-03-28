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
        KEY_FILE = credentials("css-data-jenkins")
      }

      steps {
        sh "gcloud auth activate-service-account --key-file $KEY_FILE"
        sh "gcloud config set project css-data-warehouse"
        sh "gcloud auth configure-docker"

        sh """docker build . -f docker/presto-deploy/Dockerfile \
              -t gcr.io/css-data-warehouse/presto-deploy:${IMAGE_VERSION}
           """

        sh "docker push gcr.io/css-data-warehouse/presto-deploy:${IMAGE_VERSION}"
      }
    }

    stage("Build Presto Image") {
      environment {
        IMAGE_VERSION = "${VERSION}"
        PRESTO_VERSION = "${MAJOR_VERSION}"
        HUDI_BUCKET = "css-data-warehouse-hudi-lib"
        HUDI_DIR = "jar"
        HUDI_VERSION = "0.5.0-SNAPSHOT"
        HUDI_TAG = "27f26b014235"
        JMX_EXPORTER_VERSION = "0.12.0"
        KEY_FILE = credentials("css-data-jenkins")
      }

      steps {
        sh "gcloud auth activate-service-account --key-file $KEY_FILE"
        sh "gcloud config set project css-data-warehouse"
        sh "gcloud auth configure-docker"

        sh "cp $KEY_FILE key.json"
        sh """docker build . -f docker/presto/Dockerfile \
                    --build-arg PRESTO_VERSION=$PRESTO_VERSION \
                    --build-arg JMX_EXPORTER_VERSION=${JMX_EXPORTER_VERSION} \
                    --build-arg KEY_FILE=key.json \
                    --build-arg HUDI_BUCKET=${HUDI_BUCKET} \
                    --build-arg HUDI_DIR=${HUDI_DIR} \
                    --build-arg HUDI_VERSION=${HUDI_VERSION} \
                    --build-arg HUDI_TAG=${HUDI_TAG} \
                    -t gcr.io/css-data-warehouse/presto:${IMAGE_VERSION}
                   """
        sh "docker push gcr.io/css-data-warehouse/presto:${IMAGE_VERSION}"
      }
    }
  }
}

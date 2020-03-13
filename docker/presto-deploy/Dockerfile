FROM docker.artifactory.cloudkitchens.internal/google/cloud-sdk:257.0.0

# Install Helm
RUN curl https://raw.githubusercontent.com/helm/helm/master/scripts/get > get_helm.sh &&\
    chmod 700 get_helm.sh && ./get_helm.sh --version v2.14.1 && helm init --client-only

# Install jq
RUN apt-get install -y jq

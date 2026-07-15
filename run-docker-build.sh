DOCKER_REGISTRY=ghcr.io/migas77

# help
for arg in "$@"; do
  if [ "$arg" == "--help" ]; then
    echo "Usage: cmd [-d DOCKER_REGISTRY] [--help]"
    exit 0
  fi
done

# get opts
while getopts "d:" opt; do
  case ${opt} in
    d )
        DOCKER_REGISTRY=$OPTARG
      ;;
    \? )
      echo "Invalid option: -$OPTARG" >&2
      echo "Usage: cmd [-d DOCKER_REGISTRY] [--help]"
      exit 1
      ;;
  esac
done

docker build -t $DOCKER_REGISTRY/backend -f backend/Dockerfile.backend --build-arg INSTALL_DEV=${INSTALL_DEV:-true} --build-arg INSTALL_JUPYTER=${INSTALL_JUPYTER:-true} backend
docker push $DOCKER_REGISTRY/backend
docker build -t $DOCKER_REGISTRY/report -f backend/Dockerfile.report --build-arg INSTALL_DEV=${INSTALL_DEV:-true} --build-arg INSTALL_JUPYTER=${INSTALL_JUPYTER:-true} backend
docker push $DOCKER_REGISTRY/report
docker build -t $DOCKER_REGISTRY/nginx-nef -f nginx/Dockerfile.nginx nginx
docker push $DOCKER_REGISTRY/nginx-nef

# package and publish the helm chart as an OCI artifact to the same registry
CHART_NAME=$(awk '/^name:/ { print $2; exit }' helm-chart/Chart.yaml)
CHART_VERSION=$(awk '/^version:/ { print $2; exit }' helm-chart/Chart.yaml)

helm package helm-chart -d /tmp
helm push /tmp/${CHART_NAME}-${CHART_VERSION}.tgz oci://$DOCKER_REGISTRY
rm /tmp/${CHART_NAME}-${CHART_VERSION}.tgz

# how to consume what was just published
printf "\n\n"
echo "Images and chart published to $DOCKER_REGISTRY"
echo ""
echo "Install/upgrade the release with:"
echo "  helm upgrade --install nef oci://$DOCKER_REGISTRY/$CHART_NAME --version $CHART_VERSION \\"
echo "    --set backend.deployment.image=$DOCKER_REGISTRY/backend:latest \\"
echo "    --set report.deployment.image=$DOCKER_REGISTRY/report:latest \\"
echo "    --set nginx.deployment.image=$DOCKER_REGISTRY/nginx-nef:latest"


# helm upgrade --install nef oci://ghcr.io/migas77/nef-emulator --version 0.0.1 \
#     --set backend.deployment.image=ghcr.io/migas77/backend:latest \
#     --set report.deployment.image=ghcr.io/migas77/report:latest \
#     --set nginx.deployment.image=ghcr.io/migas77/nginx-nef:latest

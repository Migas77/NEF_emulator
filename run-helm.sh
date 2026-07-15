NAMESPACE=nef
RELEASE=nef
DOCKER_REGISTRY=localhost:5000

# help
for arg in "$@"; do
  if [ "$arg" == "--help" ]; then
    echo "Usage: cmd [-n NAMESPACE] [-r RELEASE] [-d DOCKER_REGISTRY] [--help]"
    exit 0
  fi
done

# get opts
while getopts "n:r:d" opt; do
  case ${opt} in
    n )
        NAMESPACE=$OPTARG
      ;;
    r )
        RELEASE=$OPTARG
      ;;
    d )
        DOCKER_REGISTRY=$OPTARG
      ;;
    \? )
      echo "Invalid option: -$OPTARG" >&2
      echo "Usage: cmd [-n NAMESPACE] [-r RELEASE] [-d DOCKER_REGISTRY] [--help]"
      exit 1
      ;;
  esac
done

kubectl create namespace $NAMESPACE

bash run-docker-build.sh -d $DOCKER_REGISTRY
helm -n $NAMESPACE upgrade --wait --install $RELEASE helm-chart --set backend.deployment.image=$DOCKER_REGISTRY/backend:latest --set report.deployment.image=$DOCKER_REGISTRY/report:latest --set nginx.deployment.image=$DOCKER_REGISTRY/nginx-nef:latest

# get the external IP address of the service nef-backend if it exists, otherwise the cluster IP
NEF_BACKEND_IP=$(kubectl get svc $RELEASE-backend -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
if [ -z "$NEF_BACKEND_IP" ]; then
    NEF_BACKEND_IP=$(kubectl get svc $RELEASE-backend -n $NAMESPACE -o jsonpath='{.spec.clusterIP}')
fi

# Get the service port
NEF_BACKEND_PORT=$(kubectl get svc $RELEASE-backend -n $NAMESPACE -o jsonpath='{.spec.ports[0].port}')

# Get the report service port
NEF_REPORT_PORT=$(kubectl get svc $RELEASE-report -n $NAMESPACE -o jsonpath='{.spec.ports[0].port}')

# Get the super user name and password
FIRST_SUPERUSER="$(kubectl get configmap $RELEASE-nef-config -n $NAMESPACE -o jsonpath='{.data.FIRST_SUPERUSER}')"
FIRST_SUPERUSER_PASSWORD="$(kubectl get secret $RELEASE-nef-secrets -n $NAMESPACE -o jsonpath='{.data.FIRST_SUPERUSER_PASSWORD}' | base64 -d)"

# Run the script to create the data
bash backend/app/app/db/init_simple.sh -h "http://$NEF_BACKEND_IP" -p "$NEF_BACKEND_PORT" -r "$NEF_REPORT_PORT" -u "$FIRST_SUPERUSER" -s "$FIRST_SUPERUSER_PASSWORD"

# echo the variables
printf "\n\n"
echo "NEF IP: $NEF_BACKEND_IP"
echo "NEF port: $NEF_BACKEND_PORT"
echo "Super user name: $FIRST_SUPERUSER"
echo "Super user password: $FIRST_SUPERUSER_PASSWORD"

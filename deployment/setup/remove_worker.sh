worker=$1

echo "draining node $worker"
kubectl drain $worker
echo "uncordoning node $worker"
kubectl uncordon $worker

echo "node $worker drained and uncordoned. Please get to worker node and sudo kubedam reset manually "

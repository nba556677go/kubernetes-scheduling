echo "Did you pass the pod CIDR? Quit otherwise"
sleep 2

pod_cidr=$1
node_name=$2
escaped_pod_cidr=$(echo "$pod_cidr" | sed  's/\//\\\//g')

wget https://raw.githubusercontent.com/coreos/flannel/master/Documentation/kube-flannel.yml
sed -i 's/"Network": "10.244.0.0\/16"/"Network": "'"$escaped_pod_cidr"'"/g' kube-flannel.yml

#disable swap
sudo swapoff -a
sudo kubeadm init --pod-network-cidr=$pod_cidr

# CONTROLLER ONLY
mkdir -p $HOME/.kube;
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config;
sudo chown $(id -u):$(id -g) $HOME/.kube/config;

sudo kubectl apply -f kube-flannel.yml
sudo systemctl status kubelet --no-pager

#sudo kubectl create -f metric_server/deployment.yml
sudo kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
count=10
for i in $(seq $count)
do
    kubectl get pods --all-namespaces;
done
kubeadm token create --print-join-command

#setup gpusharing plugin
kubectl create -f gpushare-schd-extender.yaml
#copy scheduler config to /etc
sudo cp ../config/kube-scheduler.yaml.gpushare.default /etc/kubernetes
sudo cp /etc/kubernetes/kube-scheduler.yaml.gpushare.default /etc/kubernetes/kube-scheduler.yaml
sudo mv /etc/kubernetes/kube-scheduler.yaml /etc/kubernetes/manifest
sudo systemctl restart kubelet
kubectl create -f ../config/device-plugin-rbac.yaml
kubectl create -f ../config/device-plugin-ds.yaml

#label
kubectl label node $node_name gpushare=true


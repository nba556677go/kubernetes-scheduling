cp ../_output/bin/kube-scheduler .
docker build -t nba556677/my-kube-scheduler:1.0 .
docker push nba556677/my-kube-scheduler:1.0
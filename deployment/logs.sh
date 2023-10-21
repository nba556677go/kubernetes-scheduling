shopt -s expand_aliases
source ~/.bash_aliases

name=`kgpall | grep my-scheduler | awk '{ print $2 }'`
echo "$name"
ksys logs "$name" > logs/sched.log

kubectl get events --sort-by=.metadata.creationTimestamp  > ./logs/event.log
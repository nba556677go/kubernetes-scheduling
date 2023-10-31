shopt -s expand_aliases
source ~/.bash_aliases

OUTPUTDIR="/home/bing/kubernetes-scheduling/deployment"
name="kube-scheduler-node2"
echo "$OUTPUTDIR"
rm -f $OUTPUTDIR/logs/event.log $OUTPUTDIR/logs/sched.log 
ksys logs "$name"  > $OUTPUTDIR/logs/sched.log


kubectl get events --sort-by='.lastTimestamp'  > $OUTPUTDIR/logs/event.log
/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"context"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	//schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/klog/v2"
)

// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

///// Implemenation for AlloX /////

/*************** IMPORTANT PARAMETERs *******************/
const SCHEDULER = SJF
const AlphaFair = 0.5 //
const NUM_USERS = 4

const AVG_BETA = 95.0 * MILLI // based on simulator.

var ENABLE_PROFILING = true

var USING_FIFO = false

const NUM_RESERVE_CPU_NODE = 1 //for master
const MASTER_CPU_CORES = 20    //for master

//Bing-cluster GPU info. 

var podResourceReq = make(map[*framework.QueuedPodInfo]map[string]int64) //TODO - add to types later
var GPURESERVED = 2048
var QUEUED_UP_JOBS = 3 //
var AlloX_DEBUG = true
var podTotalMilliCPUReq = 0
var	podTotalMilliGPUReq = 0
var	nodeTotalMilliCPU int64
var	nodeTotalMilliGPU = 24*1024 - GPURESERVED//TODO - switch to milli
var	nodeAvailMilliGPU = nodeTotalMilliGPU//TODO - switch to milli
// TODO: WORK AROUND  = break deadlock at begginning.
const GPU_CAP = 1
const CPU_CAP = 8*20000 + GPU_CAP*4000
const MEM_CAP = 24 * GI

const (
	ES       string = "ES"
	Static   string = "Static"   // offline
	NaiveDRF string = "NaiveDRF" // offline
	AlloX    string = "AlloX"
	DRF      string = "DRF"
	DRFFIFO  string = "DRFFIFO"
	DRFExt   string = "DRFExt"
	SJF      string = "SJF"
	// FS       string = "FS"
)

const NvidiaGPU = "nvidia.com/gpu"

type ProcessingTime struct {
	podInfo       *framework.QueuedPodInfo
	isGPU          bool
	processingTime int64
}

type Profile struct {
	GPU_MEMORY int64
	GPU_COMPTIME int64
	CPU_COMPTIME int64
}

const IS_TEST = false
const ENABLE_JOB_SCHEDULING = true
const ENABLE_ONLINE_SCHEDULER = true
const ENABLE_OFFLINE_SCHEDULER = false
const ENABLE_MOCKUP_GPU = false
const NUM_MOCKUP_GPUS_PER_NODE = 1
const NUM_RESERVE_CPU = 0
const NUM_RESERVE_GPU = 0

const PROFILING_STR = "profiling"
var CAPACITY = &framework.Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}

var FairScoreMap = make(map[string]float64)
var DEBUG = true

// for flow scheduler
var AvailableTimes = make(map[int64]int64)
var ArrivalTimes = make(map[string]int64)
var MachinePodMap = make(map[string]int64)
var PodMachineMap = make(map[int64]string)
var NumOfNodes = 0
var NumOfCPUNodes = 0
var NumOfGPUNodes = 0
var CPU_DEMAND_PER_JOB = 20

var START_TIME = time.Now()
var ARRIVAL_TIME = 0

const SECOND = 1000000000
func InitAllNameSpaces(numOfUser int) {
	for i := 1; i <= numOfUser; i++ {
		FairScoreMap["user"+strconv.Itoa(i)] = 0.0
	}

}

// var CAPACITY = &Resource{MilliCPU: 0, NvidiaGPU: 0, Memory: 0}
func checkGPUAvailability(podInfo *framework.QueuedPodInfo) bool{
	klog.V(4).InfoS("checking gpu availbility of pod", "podName", podInfo.Pod.Name) 
	if nodeAvailMilliGPU > int(podResourceReq[podInfo]["GPU_MEMORY"]){
		nodeAvailMilliGPU -= int(podResourceReq[podInfo]["GPU_MEMORY"])
		klog.V(4).InfoS("current availGPU mem", "nodeAvailMilliGPU", nodeAvailMilliGPU)
		return true
	}
	klog.V(4).InfoS("not enough GPU mem.", "nodeAvailMilliGPU", nodeAvailMilliGPU, "pod requested GPU_MEMORY", podResourceReq[podInfo]["GPU_MEMORY"])
	return false

}

func releaseGPUAvailbility(pod *v1.Pod){
	//called when GPU needs to be released
	
}

func getContainerProfile(pod *v1.Pod) map[string]int64 {
	var Profile = make(map[string]int64)
	for _, container := range pod.Spec.Containers {

		for _, element := range container.Env{
			//fmt.Println(element)
			value, _ := strconv.ParseInt(element.Value, 10, 64)
			Profile[element.Name] = value
						
		}
	}
	klog.V(4).InfoS("profile map", "profile", Profile)
	return Profile
	
}
/*func UpdateFairScore(pod *v1.Pod, isStart bool) {

	if strings.Contains(pod.Name, PROFILING_STR) {
		return
	}

	isCpu := !IsGpuPod(pod)
	p1 := GetCpuComplTime(pod)
	p2 := GetGpuComplTime(pod)
	fairVal := 0.0
	if CAPACITY.MilliCPU == 0 {
		glog.Errorf("[Bing] ERROR CAPACITY has not updated yet")
		return
	}
	if p1 < p2 {
		c, _, m := GetDemand(pod, false)
		if isCpu {
			fairVal = math.Max(float64(c)/float64(CAPACITY.MilliCPU), float64(m)/float64(CAPACITY.Memory))
		} else {
			fairVal = float64(p1) / float64(p2) * math.Max(float64(c)/float64(CAPACITY.MilliCPU), float64(m)/float64(CAPACITY.Memory))
		}
	} else {
		_, g, m := GetDemand(pod, true)
		if isCpu {
			fairVal = float64(p2) / float64(p1) * math.Max(float64(g)/float64(CAPACITY.ScalarResources[NvidiaGPU]), float64(m)/float64(CAPACITY.Memory))
		} else {
			fairVal = math.Max(float64(g)/float64(CAPACITY.ScalarResources[NvidiaGPU]), float64(m)/float64(CAPACITY.Memory))
		}
	}

	// start a pod or finish a pod
	currTime := GetCurrentTime()
	if isStart {
		FairScoreMap[pod.Namespace] += fairVal
		processTime := int64(0)
		if IsGpuPod(pod) {
			processTime = GetGpuComplTime(pod)
		} else {
			processTime = GetCpuComplTime(pod)
		}
		AvailableTimes[MachinePodMap[pod.Name]] = currTime + processTime
	} else {
		FairScoreMap[pod.Namespace] -= fairVal
		AvailableTimes[MachinePodMap[pod.Name]] = GetCurrentTime() // if this machine is available.
		// delete pod from machine.
		machineId := MachinePodMap[pod.Name]
		delete(MachinePodMap, pod.Name)
		delete(PodMachineMap, machineId)
	}
	glog.Infof("[Bing] FairScoreMap:  %v", FairScoreMap)
}*/

type FIFO struct {
	*cache.FIFO
}

var schedPodQueue = FIFO{FIFO: cache.NewFIFO(cache.MetaNamespaceKeyFunc)}

func DoNotUseReserveResource(isGPU bool) bool {
	usage, capacity := GetResourceUsageAndCapacity()
	if isGPU {
		if capacity.ScalarResources[NvidiaGPU]-usage.ScalarResources[NvidiaGPU] <= NUM_RESERVE_GPU {
			return true
		}
	} else {
		if capacity.MilliCPU-usage.MilliCPU <= NUM_RESERVE_CPU*MILLI {
			return true
		}
	}
	return false
} 

func AddToSchedPodQueue(pod *v1.Pod) {
	allPods := schedPodQueue.List()
	// isExist := false
	// var temp *v1.Pod
	for _, p := range allPods {
		if pod.Name == p.(*v1.Pod).Name {
			schedPodQueue.Delete(p)
			break
		}
	}
	schedPodQueue.AddIfNotPresent(pod)
}

func DeleteFromSchedPodQueue(pod *v1.Pod) {
	allPods := schedPodQueue.List()
	for _, p := range allPods {
		if pod.Name == p.(*v1.Pod).Name {
			schedPodQueue.Delete(p)
		}
	}
}

// Bing
func GetSecondaryDemand(pInfo *framework.QueuedPodInfo) (int64, int64, int64) {
	milliCPU := int64(0)
	gpu := int64(0)
	memInGi := int64(0)
	for _, container := range pInfo.Pod.Spec.Containers {
		// switch demands
		secDemand := container.Command[5]
		strDemands := strings.Split(secDemand, ",")
		cpuDemand, err := strconv.ParseInt(strings.TrimSpace(strDemands[0]), 10, 64)
		if err != nil {
			glog.Infof("Failed  to convert cpuDemand %s to int64", strDemands[0])
		}
		gpuDemand, err := strconv.ParseInt(strings.TrimSpace(strDemands[1]), 10, 64)
		if err != nil {
			glog.Infof("Failed to convert gpuDemand %s to int64", strDemands[1])
		}
		memory, err := strconv.ParseInt(strings.TrimSpace(strDemands[2]), 10, 64)
		if err != nil {
			glog.Infof("Failed to convert memory %s to int64", strDemands[2])
		}

		milliCPU += cpuDemand
		gpu += gpuDemand
		memInGi += memory
	}

	return milliCPU, gpu, memInGi
}
func GetPrimaryDemand(pod *v1.Pod) (int64, int64, int64) {
	milliCPU := int64(0)
	gpu := int64(0)
	memInGi := int64(0)
	for _, container := range pod.Spec.Containers {
		// switch demands
		secDemand := container.Command[4]
		strDemands := strings.Split(secDemand, ",")
		cpuDemand, err := strconv.ParseInt(strings.TrimSpace(strDemands[0]), 10, 64)
		if err != nil {
			glog.Infof("Failed  to convert cpuDemand %s to int64", strDemands[0])
		}
		gpuDemand, err := strconv.ParseInt(strings.TrimSpace(strDemands[1]), 10, 64)
		if err != nil {
			glog.Infof("Failed to convert gpuDemand %s to int64", strDemands[1])
		}
		memory, err := strconv.ParseInt(strings.TrimSpace(strDemands[2]), 10, 64)
		if err != nil {
			glog.Infof("Failed to convert memory %s to int64", strDemands[2])
		}

		milliCPU += cpuDemand
		gpu += gpuDemand
		memInGi += memory
	}

	return milliCPU, gpu, memInGi
}

var NodeNameToInfo = map[string]*framework.NodeInfo{}
var nodes = []*v1.Node{}

func InitParameters() {
	InitAllNameSpaces(NUM_USERS)
	START_TIME = time.Now()
	switch SCHEDULER {
	case DRFFIFO:
		ENABLE_PROFILING = false
		USING_FIFO = true
	}
}

func SynClusterInfo(nn map[string]*framework.NodeInfo, n []*v1.Node) {
	glog.Infof("[Bing] SynClusterInfo %v", CAPACITY)
	if nn != nil {
		NodeNameToInfo = nn
	}

	if n != nil {
		nodes = n
	}

	_, CAPACITY = GetResourceUsageAndCapacity()
}

func RequestedResources() (*framework.Resource, *framework.Resource) {
	requested := &framework.Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}
	capacity := &framework.Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}
	for _, node := range nodes {
		capTem := NodeNameToInfo[node.Name].Allocatable
		pods := NodeNameToInfo[node.Name].Pods
		for _, pod := range pods {
			iContainer := 0
			for _, container := range pod.Pod.Spec.Containers {
				requested = SumResource(requested, container.Resources.Requests, false)
				iContainer++
			}
		}
		capacity = sumRes(capacity, capTem)
	}
	// glog.Infof("[Bing] RequestedResources: %v/%v", requested, capacity)
	return requested, capacity
}

func GetResourceUsageAndCapacity() (*framework.Resource, *framework.Resource) {
	usage := &framework.Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}
	capacity := &framework.Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}

	if len(nodes) == 0 || len(NodeNameToInfo) == 0 {
		if ENABLE_MOCKUP_GPU {
			return &framework.Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}}, &framework.Resource{MilliCPU: 0, Memory: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 1}}
		}
		// workaround: makesure there is GPU at begining.
		capacity = &framework.Resource{MilliCPU: CPU_CAP, Memory: MEM_CAP, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: int64(nodeTotalMilliGPU)}}
		glog.Infof("[Bing] WORK around capacity %v", capacity)
	} else {
		allPods := make([]*v1.Pod, 0)
		for _, node := range nodes {
			// count the scheduled pods.
			capTem := NodeNameToInfo[node.Name].Allocatable
			// usageTemp := NodeNameToInfo[node.Name].RequestedResource()
			// glog.Infof("[Bing] RequestedResource %v", usageTemp)
			mockupGPUCap := int64(NUM_MOCKUP_GPUS_PER_NODE)

			pods := NodeNameToInfo[node.Name].Pods
			for _, pod := range pods {
				// DeleteFromSchedPodQueue(pod) // workaround~~
				if !ContainsPod(allPods, pod.Pod) {
					allPods = append(allPods, pod.Pod)
					for _, container := range pod.Pod.Spec.Containers {
						usage = SumResource(usage, container.Resources.Requests, false)
						if ENABLE_MOCKUP_GPU && strings.Contains(container.Image, "gpu") {
							usage.ScalarResources[NvidiaGPU]++
							mockupGPUCap--
						}
					}
				}
			}

			// usage = sumRes(usage, &usageTemp)
			capacity = sumRes(capacity, capTem)

			if ENABLE_MOCKUP_GPU {
				capacity.ScalarResources[NvidiaGPU] += mockupGPUCap
			}
		}
	}
	/*
		for _, item := range schedPodQueue.List() {
			pod := item.(*v1.Pod)
			if !ContainsPod(allPods, pod) {
				for _, container := range pod.Spec.Containers {
					usage = SumResource(usage, container.Resources.Requests, true)
					if ENABLE_MOCKUP_GPU && strings.Contains(container.Image, "gpu") {
						usage.ScalarResources[NvidiaGPU]++
						if ENABLE_MOCKUP_GPU {
							capacity.ScalarResources[NvidiaGPU]--
						}
					}
				}
			}
		}*/
	CAPACITY = capacity
	NumOfCPUNodes = int(int(capacity.MilliCPU) / MILLI / CPU_DEMAND_PER_JOB)
	NumOfGPUNodes = int(capacity.ScalarResources[NvidiaGPU])
	NumOfNodes = NumOfGPUNodes + NumOfCPUNodes - NUM_RESERVE_CPU_NODE
	return usage, capacity
}

// func updateAvailableTimes() int64 {
// 	iGpuNode := int64(0)
// 	iCpuNode := int64(NumOfGPUNodes)

// 	curTime := GetCurrentTime()
// 	for iM := 0; iM < NumOfNodes; iM++ {
// 		AvailableTimes[int64(iM)] = int64(curTime)
// 	}

// 	for _, node := range nodes {
// 		pods := NodeNameToInfo[node.Name].Pods()
// 		for _, pod := range pods {
// 			if !strings.Contains(pod.Namespace, "kube-system") {
// 				if IsGpuPod(pod) {
// 					AvailableTimes[iGpuNode] = GetGpuComplTime(pod)
// 					iGpuNode++
// 				} else {
// 					AvailableTimes[iCpuNode] = GetCpuComplTime(pod)
// 					iCpuNode++
// 				}
// 			}
// 		}
// 	}
// 	glog.Infof("[Bing] updateAvailableTimes() %v, %v/%v", AvailableTimes, NumOfGPUNodes, NumOfCPUNodes)
// 	return curTime
// }

func GetCurrentTime() int64 {
	return int64(time.Since(START_TIME) / SECOND)
}

func GetAllocatableResource() *framework.Resource {
	result := &framework.Resource{MilliCPU: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}, Memory: 0}
	for _, node := range nodes {
		// count the scheduled pods.
		nodeInfo := NodeNameToInfo[node.Name]
		temp := nodeInfo.Allocatable
		result = sumRes(result, temp)
	}
	return result
}

// GetResourceUsageByNamespace obatains the resource usage by namespace (user) //Bing
func GetResourceUsageByNamespace(ns string) *framework.Resource {
	result, allPods := GetRealResourceUsageByNamespace(ns)

	if false {
		for _, item := range schedPodQueue.List() {
			pod := item.(*v1.Pod)
			if !ContainsPod(allPods, pod) {
				if pod.Namespace == ns {
					for _, container := range pod.Spec.Containers {
						result = SumResource(result, container.Resources.Requests, false)
					}
				}
			} else {
				DeleteFromSchedPodQueue(pod) //workaround
			}
		}
		// glog.Infof("[Bing] GetResourceUsageByNamespace %v:%v", ns, result)
	}

	return result
}

func ContainsPod(allPods []*v1.Pod, pod *v1.Pod) bool {
	for _, p := range allPods {
		if p.Name == pod.Name {
			return true
		}
	}
	return false
}

func DeletePodFromCache(p *v1.Pod) {
	for _, node := range nodes {
		pods := NodeNameToInfo[node.Name].Pods
		// TODO: May not need the following lines
		for i := 0; i < len(pods); i++ {
			pod := pods[i].Pod
			if pod.Name == p.Name {
				// count the scheduled pods.
				NodeNameToInfo[node.Name].RemovePod(pod)
				// newPods := NodeNameToInfo[node.Name].pods
				// newPods[i] = newPods[len(newPods)-1]
				// newPods = newPods[:len(newPods)-1]
				// NodeNameToInfo[node.Name].pods = newPods
				return
			}
		}
	}
}

func GetRealResourceUsageByNamespace(ns string) (*framework.Resource, []*v1.Pod) {
	result := &framework.Resource{MilliCPU: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}, Memory: 0}
	if len(nodes) == 0 || len(NodeNameToInfo) == 0 {
		return result, nil
	}

	allPods := make([]*v1.Pod, 0)
	for _, node := range nodes {
		// count the scheduled pods.
		pods := NodeNameToInfo[node.Name].Pods
		for _, pod := range pods {
			if !ContainsPod(allPods, pod.Pod) {
				allPods = append(allPods, pod.Pod)
				if pod.Pod.Namespace == ns {
					for _, container := range pod.Pod.Spec.Containers {
						result = SumResource(result, container.Resources.Requests, false)
					}
				}
			}
		}
	}
	// glog.Infof("[Bing] GetRealResourceUsageByNamespace %v:%v", ns, result)
	return result, allPods
}

func Contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}
	_, ok := set[item]
	return ok
}

func GetAllNameSpaces() []string {
	namespaces := []string{"user1"}
	return namespaces
}

func GetNameSpaces() []string {
	namespaces := make([]string, 0)

	if len(nodes) == 0 || len(NodeNameToInfo) == 0 {
		return namespaces
	}

	for _, node := range nodes {
		// count the scheduled pods.
		pods := NodeNameToInfo[node.Name].Pods
		for _, pod := range pods {
			if !Contains(namespaces, pod.Pod.Namespace) {
				namespaces = append(namespaces, pod.Pod.Namespace)
			}
		}
	}

	return namespaces
}

func GetActiveUsers(pods []*v1.Pod) []string {
	namespaces := GetNameSpaces()
	for _, pod := range pods {
		if !Contains(namespaces, pod.Namespace) {
			namespaces = append(namespaces, pod.Namespace)
		}
	}
	return namespaces
}

func GetQueueUsers(pods []interface{},) []string {
	namespaces := make([]string, 0)
	for _, podInfo := range pods {
		var podName = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod.Namespace
		if !Contains(namespaces, podName) {
			namespaces = append(namespaces, podName)
		}
	}
	return namespaces
}

func GetAllUsers() []string {
	namespaces := make([]string, 0)
	for i := 1; i <= NUM_USERS; i++ {
		namespaces = append(namespaces, "user"+strconv.Itoa(i))
	}
	return namespaces
}

// IRA_Add adds ResourceList into Resource but ignore the usage of CPU demand on GPU jobs.
func SumResource(result *framework.Resource, rl v1.ResourceList, ignoreGPUcpu bool) *framework.Resource {
	milliCPU := int64(0)
	nvidiaGPU := int64(0)
	memory := int64(0)
	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			milliCPU += rQuant.MilliValue()
		case v1.ResourceMemory:
			memory += rQuant.Value()
		case NvidiaGPU:
			nvidiaGPU += rQuant.Value()
			// default:
			// 	glog.Infof("[Bing] resource: %v=%v", rName, rQuant)
		}
	}
	result.ScalarResources[NvidiaGPU] += nvidiaGPU
	result.Memory += memory
	if ignoreGPUcpu {
		if nvidiaGPU == 0 { // ignore cpu usage of GPU jobs
			result.MilliCPU += milliCPU
		}
	} else {
		result.MilliCPU += milliCPU
	}
	return result
}

func sumRes(a *framework.Resource, b *framework.Resource) *framework.Resource {
	result := &framework.Resource{MilliCPU: 0, ScalarResources: map[v1.ResourceName]int64{NvidiaGPU: 0}, Memory: 0}
	result.ScalarResources[NvidiaGPU] = a.ScalarResources[NvidiaGPU] + b.ScalarResources[NvidiaGPU]
	result.MilliCPU = a.MilliCPU + b.MilliCPU
	result.Memory = a.Memory + b.Memory
	result.EphemeralStorage = a.EphemeralStorage + b.EphemeralStorage
	return result
}

func GetDemand(pod *v1.Pod, isGpu bool) (int64, int64, int64) {
	milliCPU := int64(0)
	gpu := int64(0)
	memInGi := int64(0)
	for _, container := range pod.Spec.Containers {
		cmdIdx := 4
		if isGpu && strings.Contains(container.Image, "gpu") {
			cmdIdx = 4 // primary
		} else {
			cmdIdx = 5 // secondary
		}

		// switch demands
		secDemand := container.Command[cmdIdx]

		strDemands := strings.Split(secDemand, ",")
		cpuDemand, err := strconv.ParseInt(strDemands[0], 10, 64)
		if err != nil {
			glog.Infof("Failed  to convert cpuDemand %s to int64", strDemands[0])
		}
		gpuDemand, err := strconv.ParseInt(strDemands[1], 10, 64)
		if err != nil {
			glog.Infof("Failed to convert gpuDemand %s to int64", strDemands[1])
		}
		memory, err := strconv.ParseInt(strDemands[2], 10, 64)
		if err != nil {
			glog.Infof("Failed to convert memory %s to int64", strDemands[2])
		}

		milliCPU += cpuDemand
		gpu += gpuDemand
		memInGi += memory
	}

	return milliCPU, gpu, memInGi
}

// Bing
func GetGpuComplTime(profile map[string]int64) int64 {
	return profile["GPU_COMPTIME"]
}

/*func GetShortestComplTime(pod *v1.Pod) (int64, bool) {
	isGPU := true
	cpuComplt := int64(GetCpuComplTime(pod))
	shortestComplt := int64(GetGpuComplTime(pod))
	if shortestComplt > cpuComplt {
		isGPU = false
		shortestComplt = cpuComplt
	}

	return shortestComplt, isGPU
}*/

func GetCpuComplTime(Profile map[string]int64) int64 {
	return Profile["CPU_COMPTIME"]
}

const MILLI = 1000
const GI = 1024 * 1024 * 1024

func IsGpuPod(pod *v1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		if strings.Contains(container.Command[2], "cuda") {
			return true
		}
	}
	return false
}

// func (g *genericScheduler) SubmitOnOtherDevice(pod *v1.Pod, replicatedPod *v1.Pod) {
// 	pod.Spec.Containers = replicatedPod.Spec.Containers

// 	// p.Client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{})
// 	if err := g.client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{}); err != nil {
// 		runtime.HandleError(fmt.Errorf("unable to DELETE pod in kubectl %T: %v", pod, err))
// 	}
// 	if _, err := g.client.CoreV1().Pods(replicatedPod.Namespace).Create(replicatedPod); err != nil {
// 		runtime.HandleError(fmt.Errorf("unable to CREATE pod in kubectl %T: %v", replicatedPod, err))
// 	}
// }

func SubmitOnOtherDevice(client clientset.Interface, pInfo *framework.QueuedPodInfo, replicatedpInfo *framework.QueuedPodInfo) {
	// pod.Spec.Containers = replicatedPod.Spec.Containers
	// foregroundDelete := metav1.DeletePropagationForeground
	// if err := client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{PropagationPolicy: &foregroundDelete}); err != nil {
	if err := client.CoreV1().Pods(pInfo.Pod.Namespace).Delete(context.TODO(), pInfo.Pod.Name, metav1.DeleteOptions{}); err != nil {
		runtime.HandleError(fmt.Errorf("unable to DELETE pod in kubectl %T: %v", pInfo.Pod, err))
	}
	if _, err := client.CoreV1().Pods(replicatedpInfo.Pod.Namespace).Create(context.TODO(), replicatedpInfo.Pod, metav1.CreateOptions{}); err != nil {
		runtime.HandleError(fmt.Errorf("unable to CREATE pod in kubectl %T: %v", replicatedpInfo.Pod, err))
	}
	// if p, err := client.CoreV1().Pods(replicatedPod.Namespace).Update(replicatedPod); err == nil {
	// 	pod = p
	// } else {
	// 	// this does notwork --> cannot update resource requests.
	// 	runtime.HandleError(fmt.Errorf("unable to Update pod in kubectl %T: %v", replicatedPod, err))
	// }
}


func CreatePodOnOtherDevice(pInfo *framework.QueuedPodInfo, toBeGPU bool) (*framework.QueuedPodInfo, bool) {
	
	// check the device of the pod
	//Bing - try to directly return pInfo
	return pInfo, false
	/*
	for _, container := range pInfo.Pod.Spec.Containers {
		if strings.Contains(container.Image, "gpu") && toBeGPU {
			return pInfo, false
		}
		if strings.Contains(container.Image, "cpu") && !toBeGPU {
			return pInfo, false
		}
	}
	
	replicatedPodInfo := pInfo.DeepCopy()
	for cName, container := range replicatedPodInfo.Pod.Spec.Containers {
		if toBeGPU {
			container.Image = "lenhattan86/ira:gpu"
		} else {
			container.Image = "lenhattan86/ira:cpu"
		}
		// switch commands
		mainCmd := container.Command[3]
		container.Command[3] = container.Command[2]
		container.Command[2] = mainCmd
		// switch demands
		mainDemand := container.Command[5]
		container.Command[5] = container.Command[4]
		container.Command[4] = mainDemand

		cpuDemand, gpuDemand, memory := GetSecondaryDemand(pInfo)

		for rName := range container.Resources.Requests {
			quantity := container.Resources.Requests[rName]
			switch rName {
			case v1.ResourceCPU:
				quantity.SetMilli(cpuDemand)
			case NvidiaGPU:
				if ENABLE_MOCKUP_GPU {
					quantity.Set(0)
				} else {
					quantity.Set(gpuDemand)
				}
			case v1.ResourceMemory:
				quantity.Set(memory * GI)
			}
			container.Resources.Requests[rName] = quantity
			container.Resources.Limits[rName] = quantity
		}
		replicatedPodInfo.Pod.Spec.Containers[cName] = container
	}

	replicatedPodInfo.Pod.ResourceVersion = ""
	replicatedPodInfo.Pod.Spec.NodeName = ""
	replicatedPodInfo.Pod.Annotations = nil
	// replicatedPod.ResourceVersion = pod.ResourceVersion
	// replicatedPod.Spec.NodeName = pod.Spec.NodeName
	// replicatedPod.Annotations = pod.Annotations
	
	return replicatedPodInfo, true
	*/
}

/*func EqualShare(allPods []interface{}, client clientset.Interface) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	users := GetQueueUsers(allPods)
	// glog.Infof("[Bing] active users: %v", users)
	// get resource usage on each user
	resourceMap := make(map[string]*framework.Resource)
	for _, user := range users {
		resourceMap[user] = GetResourceUsageByNamespace(user)
	}

	var pod *v1.Pod
	shortestGPUComplt := int64(math.MaxInt64)
	isGPUAvailable := false
	usage, capacity := GetResourceUsageAndCapacity()
	// glog.Infof("[Bing] GetResourceUsageAndCapacity usage/capacity %v/", usage, capacity)

	isGPUAvailable = (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvailable := (capacity.MilliCPU - usage.MilliCPU) > (NUM_RESERVE_CPU)*MILLI

	if isGPUAvailable {
		gpuShare := capacity.ScalarResources[NvidiaGPU] / int64(NUM_USERS)
		var minUser string
		minGpuUsage := int64(math.MaxInt64)
		// pick the user with the least GPU
		for _, user := range users {
			gpuTemp := resourceMap[user].ScalarResources[NvidiaGPU]
			if minGpuUsage > gpuTemp {
				minUser = user
				minGpuUsage = gpuTemp
			}
		}

		// static allocation
		if minGpuUsage < gpuShare {
			// glog.Infof("[Bing] pick user: %v", minUser)
			// pick the pod with the shortest GPU complt.
			// pod := allPods[0]
			if USING_FIFO {
				for _, podInfo := range allPods {
					var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
					if minUser == p.Namespace {
						pod = p
						// isGPU = IsGpuPod(pod)
						break
					}
				}
			} else {
				for _, podInfo := range allPods {
					var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
					complTime := GetGpuComplTime(p)
					if minUser == p.Namespace && GetGpuComplTime(p) < shortestGPUComplt {
						pod = p
						shortestGPUComplt = complTime
					}
				}
			}
			repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			// glog.Infof("[Bing] pick pod: %v/%v on GPU: %v", repPod.Namespace, repPod.Name, IsGpuPod(repPod))
			return pod
		}

	}

	if isCPUAvailable {
		cpuShare := (capacity.MilliCPU - (MASTER_CPU_CORES * MILLI)) / int64(NUM_USERS)
		roundCPUShare := cpuShare / int64(CPU_DEMAND_PER_JOB*1000)

		var minUser string
		minCpuUsage := int64(math.MaxInt64)
		// pick the user with the least CPU
		for _, user := range users {
			cpuTemp := resourceMap[user].MilliCPU
			if minCpuUsage > cpuTemp {
				minUser = user
				minCpuUsage = cpuTemp
			}
		}

		// static allocation
		if minCpuUsage/int64((CPU_DEMAND_PER_JOB-1)*1000) < roundCPUShare {
			// glog.Infof("[Bing] pick user: %v", minUser)
			// pick the pod with the shortest CPU complt.
			if USING_FIFO {
				for _, podInfo := range allPods {
					var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
					if minUser == p.Namespace {
						pod = p
						// isGPU = IsGpuPod(pod)
						break
					}
				}
			} else {
				shortestCPUComplt := int64(math.MaxInt64)
				for _, podInfo := range allPods {
					var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
					complTime := GetCpuComplTime(p)
					if minUser == p.Namespace && GetCpuComplTime(p) < shortestCPUComplt {
						pod = p
						shortestCPUComplt = complTime
					}
				}
			}
			repPod, isRep := CreatePodOnOtherDevice(pod, false)
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			// glog.Infof("[Bing] pick pod: %v/%v on CPU: %v", repPod.Namespace, repPod.Name, !IsGpuPod(repPod))
			return pod
		}
	}

	return nil
}*/

// EqualShare: ES
/*func OnlineEqualShare(allPods []interface{}, client clientset.Interface) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	// glog.Infof("[Bing] active users: %v", activeUsers)
	// get resource usage on each user
	resourceMap := make(map[string]*framework.Resource)
	for _, user := range activeUsers {
		resourceMap[user] = GetResourceUsageByNamespace(user)
		// resourceMap[user] = GetRealResourceUsageByNamespace(user)
		// glog.Infof("[Bing] resourceMap[%v]:%v", user, resourceMap[user])
	}

	var pod *v1.Pod
	shortestGPUComplt := int64(math.MaxInt64)
	isGPUAvailable := false
	usage, capacity := GetResourceUsageAndCapacity()

	// do nothing if capacity is not syned.
	// if capacity.MilliCPU == 0 {
	// 	return nil
	// }

	isGPUAvailable = (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvailable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU*MILLI

	if isGPUAvailable {
		var minUser string
		minGpuUsage := int64(math.MaxInt64)
		// pick the user with the least GPU
		for _, user := range activeUsers {
			gpuTemp := resourceMap[user].ScalarResources[NvidiaGPU]
			if minGpuUsage > gpuTemp {
				minUser = user
				minGpuUsage = gpuTemp
			}
		}
		// glog.Infof("[Bing] pick user: %v", minUser)
		// pick the pod with the shortest GPU complt.
		// pod := allPods[0]
		if USING_FIFO {
			for _, podInfo := range allPods {
				var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
				if minUser == p.Namespace {
					pod = p
					// isGPU = IsGpuPod(pod)
					break
				}
			}
		} else {
			for _, podInfo := range allPods {
				var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
				complTime := GetGpuComplTime(p)
				if minUser == p.Namespace && GetGpuComplTime(p) < shortestGPUComplt {
					pod = p
					shortestGPUComplt = complTime
				}
			}
		}
		repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
		if isRep {
			SubmitOnOtherDevice(client, pod, repPod)
			pod = repPod
		}
		// glog.Infof("[Bing] pick pod: %v/%v on GPU: %v", repPod.Namespace, repPod.Name, IsGpuPod(repPod))
		return pod
	} else if isCPUAvailable {
		var minUser string
		minCpuUsage := int64(math.MaxInt64)
		// pick the user with the least CPU
		for _, user := range activeUsers {
			cpuTemp := resourceMap[user].MilliCPU
			if minCpuUsage > cpuTemp {
				minUser = user
				minCpuUsage = cpuTemp
			}
		}
		// glog.Infof("[Bing] pick user: %v", minUser)
		// pick the pod with the shortest CPU complt.
		if USING_FIFO {
			for _, podInfo := range allPods {
				var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
				if minUser == p.Namespace {
					pod = p
					// isGPU = IsGpuPod(pod)
					break
				}
			}
		} else {
			shortestCPUComplt := int64(math.MaxInt64)
			for _, podInfo := range allPods {
				var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
				complTime := GetCpuComplTime(p)
				if minUser == p.Namespace && GetCpuComplTime(p) < shortestCPUComplt {
					pod = p
					shortestCPUComplt = complTime
				}
			}
		}
		repPod, isRep := CreatePodOnOtherDevice(pod, false)
		if isRep {
			SubmitOnOtherDevice(client, pod, repPod)
			pod = repPod
		}
		// glog.Infof("[Bing] pick pod: %v/%v on CPU: %v", repPod.Namespace, repPod.Name, !IsGpuPod(repPod))
		return pod
	}

	return nil
}*/

/*func OnlineDRF(allPods []interface{}, client clientset.Interface) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	_, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	if capacity.MilliCPU == 0 {
		glog.Infof("[Bing] OnlineDRF %v", capacity)
		return nil
	}
	// glog.Infof("[Bing] active users: %v", activeUsers)
	// get resource usage on each user
	resourceMap := make(map[string]*framework.Resource)
	for _, user := range activeUsers {
		resourceMap[user] = GetResourceUsageByNamespace(user)
	}
	var minUser string
	minDominantShare := math.MaxFloat64

	// pick the user with the least Dorminant Share
	for _, user := range activeUsers {
		cpuTemp := 0.0
		gpuTemp := 0.0
		memTemp := 0.0
		if capacity.ScalarResources[NvidiaGPU] > 0 {
			gpuTemp = float64(resourceMap[user].ScalarResources[NvidiaGPU]) / float64(capacity.ScalarResources[NvidiaGPU])
		}
		if capacity.MilliCPU > 0 {
			cpuTemp = float64(resourceMap[user].MilliCPU) / float64(capacity.MilliCPU)
			memTemp = float64(resourceMap[user].Memory) / float64(capacity.Memory)
		}
		dominantShare := math.Max(math.Max(cpuTemp, gpuTemp), memTemp)
		if minDominantShare > dominantShare {
			minUser = user
			minDominantShare = dominantShare
		}
	}
	// glog.Infof("[Bing] pick user: %v", minUser)
	// pick the pod with the shortest GPU complt.
	// pod := allPods[0]
	isGPU := true
	var pod *v1.Pod
	if USING_FIFO {
		// Always put this pod on GPU.
		isGPU = true
		// match FIFO with the simulator :: WORKAROUND
		sort.SliceStable(allPods, func(i, j int) bool {
			pod1 := allPods[i].(*framework.QueuedPodInfo).PodInfo.Pod
			pod2 := allPods[j].(*framework.QueuedPodInfo).PodInfo.Pod
			if len(pod1.Name) != len(pod2.Name) {
				return len(pod1.Name) < len(pod2.Name)
			}
			return pod1.Name < pod2.Name
		})

		for _, podInfo := range allPods {
			var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
			if minUser == p.Namespace {
				pod = p
				// _, isGPU = GetShortestComplTime(p)
				break
			}
		}
	} else {
		shortestComplt := int64(math.MaxInt64)
		// glog.Infof("[Bing] usage/capacity %v/%v", usage, capacity)
		complTime := shortestComplt
		for _, podInfo := range allPods {
			var p = podInfo.(*framework.QueuedPodInfo).PodInfo.Pod
			isGPUTemp := true
			complTime, isGPUTemp = GetShortestComplTime(p)
			if minUser == p.Namespace && complTime < shortestComplt {
				pod = p
				shortestComplt = complTime
				isGPU = isGPUTemp
			}
		}
	}

	// glog.Infof("[Bing] pick pod: %v/%v isGPU: %v", pod.Namespace, pod.Name, IsGpuPod(pod))
	repPod, isRep := CreatePodOnOtherDevice(pod, isGPU) //put job on GPU
	if isRep {
		SubmitOnOtherDevice(client, pod, repPod)
		pod = repPod
	}

	return pod
}*/

/*func OnlineDRFExt(allPods []interface{}, client clientset.Interface) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	usage, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	// if capacity.MilliCPU == 0 {
	// 	return nil
	// }
	// virtualCpuUsage := usage.MilliCPU + usage.NvidiaGPU*AVG_BETA
	virtualCPUCap := capacity.MilliCPU + capacity.ScalarResources[NvidiaGPU]*AVG_BETA
	// glog.Infof("[Bing] active users: %v", activeUsers)
	// get resource usage on each user
	resourceMap := make(map[string]*framework.Resource)
	for _, user := range activeUsers {
		resourceMap[user] = GetResourceUsageByNamespace(user)
		// resourceMap[user] = GetRealResourceUsageByNamespace(user)
		// glog.Infof("[Bing] resourceMap[%v]:%v", user, resourceMap[user])
	}
	var minUser string
	minDominantShare := math.MaxFloat64

	// pick the user with the least Dorminant Share
	for _, user := range activeUsers {
		cpuTemp := float64(resourceMap[user].MilliCPU)
		gpuTemp := float64(resourceMap[user].ScalarResources[NvidiaGPU])
		memTemp := 0.0

		virtualCPU := 0.0
		if capacity.Memory > 0 {
			memTemp = float64(resourceMap[user].Memory) / float64(capacity.Memory)
			virtualCPU = (cpuTemp + gpuTemp*AVG_BETA) / float64(virtualCPUCap)
		}

		dominantShare := math.Max(virtualCPU, memTemp)
		if minDominantShare > dominantShare {
			minUser = user
			minDominantShare = dominantShare
		}
	}
	// glog.Infof("[Bing] pick user: %v", minUser)

	isGPUAvailable := (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvailable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU*MILLI
	var pod *v1.Pod
	if isGPUAvailable {
		shortestGPUComplt := int64(math.MaxInt64)
		for _, pod := range allPods {
			var p = pod.(*framework.QueuedPodInfo).PodInfo.Pod
			complTime := GetGpuComplTime(p)
			if minUser == p.Namespace && complTime < shortestGPUComplt {
				pod = p
				shortestGPUComplt = complTime
			}
		}
		repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
		if isRep {
			SubmitOnOtherDevice(client, pod, repPod)
			pod = repPod
		}
		// glog.Infof("[Bing] pick pod: %v/%v on GPU: %v", pod.Namespace, pod.Name, IsGpuPod(pod))
		return pod
	} else if isCPUAvailable {
		shortestCPUComplt := int64(math.MaxInt64)
		for _, pod := range allPods {
			var p = pod.(*framework.QueuedPodInfo).PodInfo.Pod
			complTime := GetCpuComplTime(p)
			if minUser == p.Namespace && complTime < shortestCPUComplt {
				pod = p
				shortestCPUComplt = complTime
			}
		}
		repPod, isRep := CreatePodOnOtherDevice(pod, false) //put job on CPU
		if isRep {
			SubmitOnOtherDevice(client, pod, repPod)
			pod = repPod
		}
		// glog.Infof("[Bing] pick pod: %v/%v on CPU: %v", pod.Namespace, pod.Name, !IsGpuPod(pod))
		return pod
	}

	return nil
}*/

var NEXT_ROUND = true
var MIN_USERS = make([]string, 0)

func IsProfilingJob(pod *v1.Pod) bool {
	if strings.Contains(pod.Name, PROFILING_STR) {
		return true
	}
	return false
}

func ProfilingSchedule(allPods []*v1.Pod) *v1.Pod {
	if !ENABLE_PROFILING {
		return nil
	}

	if len(allPods) > 0 {
		for _, pod := range allPods {
			if IsProfilingJob(pod) {
				return pod
			}
		}
	}
	return nil
}

/*func OnlineAlloX_pickUser(allPods []interface{}, client clientset.Interface, alpha float64) *v1.Pod {
	// get the list of active users (having jobs queued up.)
	if NEXT_ROUND {
		activeUsers := GetQueueUsers(allPods)
		nUsers := int(math.Ceil(alpha * float64(len(activeUsers))))
		if len(activeUsers) < nUsers {
			nUsers = len(activeUsers)
		}
		// sort active users based con fair score list
		sort.SliceStable(activeUsers, func(i, j int) bool {
			user1 := activeUsers[i]
			user2 := activeUsers[j]
			score1 := FairScoreMap[user1]
			score2 := FairScoreMap[user2]
			return score1 < score2
		})

		// pick the set of users with lowest fairscore
		minUsers := make([]string, 0)

		for i := 0; i < nUsers; i++ {
			minUsers = append(minUsers, activeUsers[i])
		}
		MIN_USERS = minUsers
		NEXT_ROUND = false
	}

	// glog.Infof("[Bing] allox picks users: %v", MIN_USERS)
	usage, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	if capacity.MilliCPU == 0 {
		return nil
	}
	isGPUAvailable := (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	isCPUAvailable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU*MILLI
	// isCPUAvailable := GetAllocatableResource().MilliCPU > 0
	var pod *v1.Pod

	if !isGPUAvailable && !isCPUAvailable {
		NEXT_ROUND = true
		return pod
	}

	ProcessingTimes := []ProcessingTime{}
	for _, pod := range allPods {
		var p = pod.(*framework.QueuedPodInfo).PodInfo.Pod
		for _, user := range MIN_USERS {
			if p.Namespace == user {
				gpuProcessingTime := ProcessingTime{p, true, GetGpuComplTime(p)}
				cpuProcessingTime := ProcessingTime{p, false, GetCpuComplTime(p)}
				ProcessingTimes = append(ProcessingTimes, gpuProcessingTime)
				ProcessingTimes = append(ProcessingTimes, cpuProcessingTime)
			}
		}
	}
	if len(ProcessingTimes) < 1 {
		NEXT_ROUND = true
		return pod
	}

	sort.SliceStable(ProcessingTimes, func(i, j int) bool {
		p1 := ProcessingTimes[i].processingTime
		p2 := ProcessingTimes[j].processingTime
		return p1 < p2
	})

	for _, pt := range ProcessingTimes {
		if pt.isGPU && isGPUAvailable {
			pod = pt.pod
			repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			break
		} else if !pt.isGPU && isCPUAvailable {
			pod = pt.pod
			repPod, isRep := CreatePodOnOtherDevice(pod, false) //put job on CPU
			if isRep {
				SubmitOnOtherDevice(client, pod, repPod)
				pod = repPod
			}
			break
		}
	}
	// glog.Infof("[Bing] pick pod: %v/%v on isGPU: %v", pod.Namespace, pod.Name, IsGpuPod(pod))
	return pod
}*/

func OnlineSJF(allPods []interface{}, client clientset.Interface, alpha float64) *framework.QueuedPodInfo {
	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	nUsers := int(math.Ceil(alpha * float64(len(activeUsers))))
	if len(activeUsers) < nUsers {
		nUsers = len(activeUsers)
	}
	// sort active users based con fair score list
	sort.SliceStable(activeUsers, func(i, j int) bool {
		user1 := activeUsers[i]
		user2 := activeUsers[j]
		score1 := FairScoreMap[user1]
		score2 := FairScoreMap[user2]
		return score1 < score2
	})
	/*
	klog.V(4).InfoS("[Bing]fairscore map", "fairscoreMap", FairScoreMap)
	usage, capacity := GetResourceUsageAndCapacity()
	klog.V(4).InfoS("[Bing]usage", usage,"capacity", capacity)
	// do nothing if capacity is not syned.
	if capacity.MilliCPU == 0 {
		return nil
	}*/
	// pick the set of users with lowest fairscore
	minUsers := make([]string, 0)

	for i := 0; i < nUsers; i++ {
		minUsers = append(minUsers, activeUsers[i])
	}
	klog.V(4).InfoS("[Bing]user list sorted with lowest fairscore", "minUsers", minUsers )
	// glog.Infof("[Bing] FairScoreMap:  %v", FairScoreMap)
	// glog.Infof("[Bing] allox picks users: %v", minUsers)
	//isGPUAvailable := (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	//isCPUAvailable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU*MILLI
	//default true 
	var isGPUAvailable = true
	var isCPUAvailable = true
	klog.V(4).InfoS("[Bing]GPU / CPU available flag", "isGPUAvailable",isGPUAvailable, "isCPUAvailable", isCPUAvailable )
	// isCPUAvailable := GetAllocatableResource().MilliCPU > 0
	var podInfo *framework.QueuedPodInfo

	ProcessingTimes := []ProcessingTime{}
	for _, pInfo := range allPods {
		for _, user := range minUsers {
			var pod = pInfo.(*framework.QueuedPodInfo).Pod
			if pod.Namespace == user {
				var ContainerProfile  = getContainerProfile(pod)
				klog.V(4).InfoS("[Bing] container profile", "Containerprofile", ContainerProfile)
				if podResourceReq[pInfo.(*framework.QueuedPodInfo)] == nil{
					podResourceReq[pInfo.(*framework.QueuedPodInfo)] = make(map[string]int64)
				}
				podResourceReq[pInfo.(*framework.QueuedPodInfo)] = ContainerProfile
				gpuProcessingTime := ProcessingTime{pInfo.(*framework.QueuedPodInfo), true, GetGpuComplTime(ContainerProfile)}
				cpuProcessingTime := ProcessingTime{pInfo.(*framework.QueuedPodInfo), false, GetCpuComplTime(ContainerProfile)}
				ProcessingTimes = append(ProcessingTimes, gpuProcessingTime)
				ProcessingTimes = append(ProcessingTimes, cpuProcessingTime)
			}
		}
	}
	klog.V(4).InfoS("[Bing] ProcessingTimes", ProcessingTimes)
	if len(ProcessingTimes) < 1 {
		
		return podInfo
	}
	sort.SliceStable(ProcessingTimes, func(i, j int) bool {
		p1 := ProcessingTimes[i].processingTime
		p2 := ProcessingTimes[j].processingTime
		return p1 < p2
	})
	klog.V(4).InfoS("[Bing] sorted ProcessingTimes", ProcessingTimes)
	for _, pt := range ProcessingTimes {
		if pt.isGPU &&  isGPUAvailable {
			podInfo = pt.podInfo
			klog.V(4).InfoS("[Bing] placing pod on GPU", "pod", podInfo.Pod.Name)
			repPod, isRep := CreatePodOnOtherDevice(podInfo, true) //put job on GPU
			if isRep {
				SubmitOnOtherDevice(client, podInfo, repPod)
				podInfo = repPod
			}
			
			break
		} else if !pt.isGPU && isCPUAvailable {
			podInfo = pt.podInfo
			klog.V(4).InfoS("[Bing] placing pod on CPU", "pod", podInfo.Pod.Name)
			repPodInfo, isRep := CreatePodOnOtherDevice(podInfo, false) //put job on CPU
			if isRep {
				SubmitOnOtherDevice(client, podInfo, repPodInfo)
				podInfo = repPodInfo
			}
			break
		}
		klog.V(4).InfoS("current podinfo in for loop processtimes", "podinfo", podInfo.Pod.Name)
	}
	glog.Infof("[Bing] pick pod: %v/%v on isGPU: %v, checkGPUaviailbility: %v", podInfo.Pod.Namespace, podInfo.Pod.Name, IsGpuPod(podInfo.Pod), checkGPUAvailability(podInfo))

	
	return podInfo
}

/*func OnlineAllox(allPods []interface{}, client clientset.Interface, alpha float64) *v1.Pod {
	_, capacity := GetResourceUsageAndCapacity()
	// do nothing if capacity is not syned.
	if capacity.MilliCPU == 0 {
		glog.Errorf("[Bing] OnlineAlloX: %v ==> deadlock", capacity)
		return nil
	}

	numAvailMachines := 0
	for iM := NumOfNodes - 1; iM >= 0; iM-- {
		_, busy := PodMachineMap[int64(iM)]
		if !busy {
			numAvailMachines++
		}
	}
	if numAvailMachines == 0 {
		return nil
	}

	// do nothing if no available machines.

	// get the list of active users (having jobs queued up.)
	activeUsers := GetQueueUsers(allPods)
	// nUsers := int(math.Ceil(alpha * float64(len(activeUsers))))
	nUsers := int(math.Ceil(alpha * NUM_USERS))
	nUsers = int(math.Min(float64(nUsers), float64(len(activeUsers))))
	nJobs := len(allPods)
	if len(activeUsers) < nUsers {
		nUsers = len(activeUsers)
	}
	// glog.Infof("[Bing] FairScoreMap:  %v", FairScoreMap)
	// sort active users based con fair score list
	sort.SliceStable(activeUsers, func(i, j int) bool {
		user1 := activeUsers[i]
		user2 := activeUsers[j]
		score1 := FairScoreMap[user1]
		score2 := FairScoreMap[user2]
		return score1 < score2
	})

	// pick the set of users with lowest fairscore
	minUsers := make([]string, 0)

	for i := 0; i < nUsers; i++ {
		minUsers = append(minUsers, activeUsers[i])
	}

	// glog.Infof("[Bing] allox picks users: %v with alpha %v", minUsers, alpha)
	// isGPUAvailable := (capacity.ScalarResources[NvidiaGPU] - usage.ScalarResources[NvidiaGPU]) > NUM_RESERVE_GPU
	// isCPUAvailable := (capacity.MilliCPU - usage.MilliCPU) > NUM_RESERVE_CPU
	// isCPUAvailable := GetAllocatableResource().MilliCPU > 0
	var pod *v1.Pod
	// step 1: update AvailableTimes

	currentTime := GetCurrentTime()
	for iM, aTime := range AvailableTimes {
		if aTime < currentTime {
			AvailableTimes[iM] = currentTime
		}
	}

	// step 2: create D, P, and Q
	D := make([][]int, NumOfNodes)
	P := make([][]int, NumOfNodes)
	for i := 0; i < NumOfNodes; i++ {
		D[i] = make([]int, nJobs)
		P[i] = make([]int, nJobs)
		for j := 0; j < nJobs; j++ {
			D[i][j] = 0
			temp := int(AvailableTimes[int64(i)]) - int(ArrivalTimes[allPods[j].(*framework.QueuedPodInfo).PodInfo.Pod.Name]) // TODO: use arrival time of a job instead of START_TIME.
			if temp > 0 {
				D[i][j] = temp
			}
			if i < NumOfGPUNodes {
				P[i][j] = int(GetGpuComplTime(allPods[j].(*framework.QueuedPodInfo).PodInfo.Pod))
			} else {
				P[i][j] = int(GetCpuComplTime(allPods[j].(*framework.QueuedPodInfo).PodInfo.Pod))
			}
		}
	}
	// create matrix Q

	Q := make([][]int, NumOfNodes*nJobs)
	for i := 0; i < NumOfNodes*nJobs; i++ {
		Q[i] = make([]int, nJobs)
		for j := 0; j < nJobs; j++ {
			iN := i/NumOfNodes + 1
			iM := i % NumOfNodes
			Q[i][j] = D[iM][j] + iN*P[iM][j]
		}
	}
	// step 3: solve HungarianAlgorithm
	if AlloX_DEBUG {
		glog.Infof("input  D: %v", D)
		glog.Infof("input  P: %v", P)
	}
	sols, _ := schedutil.Solve(Q)
	if AlloX_DEBUG {
		glog.Infof("input  sols: %v", sols)
		AlloX_DEBUG = false
	}
	if len(sols) < 1 {
		glog.Errorf("P: %v", P)
		glog.Errorf("Delay matrix: %v", D)
		glog.Infof("input  AvailableTimes: %v", AvailableTimes)
		glog.Infof("input  ArrivalTimes: %v", ArrivalTimes)
		glog.Infof("input  Q: %v", Q)
		glog.Infof("solution: %v", sols)
		return nil
	}
	// step 4: find the pod to be scheduled
	for iM := NumOfNodes - 1; iM >= 0; iM-- {
		for k := nJobs - 1; k >= 0; k-- {
			j := sols[k*NumOfNodes+iM]
			// check if the machine is available
			_, busy := PodMachineMap[int64(iM)]
			if j >= 0 && !busy {
				if iM < NumOfGPUNodes {
					pod = allPods[j].(*framework.QueuedPodInfo).PodInfo.Pod
					repPod, isRep := CreatePodOnOtherDevice(pod, true) //put job on GPU
					if isRep {
						SubmitOnOtherDevice(client, pod, repPod)
						pod = repPod
					}
					// glog.Infof("[Bing] pick pod: %v/%v on GPU: %v on node %v in %v seconds", pod.Namespace, pod.Name, IsGpuPod(pod), iM, GetGpuComplTime(pod))
					MachinePodMap[pod.Name] = int64(iM)
					PodMachineMap[int64(iM)] = pod.Name
					return pod
				} else {
					pod = allPods[j].(*framework.QueuedPodInfo).PodInfo.Pod
					repPod, isRep := CreatePodOnOtherDevice(pod, false) //put job on CPU
					if isRep {
						SubmitOnOtherDevice(client, pod, repPod)
						pod = repPod
					}
					// glog.Infof("[Bing] pick pod: %v/%v on CPU: %v on node %v in %v seconds", pod.Namespace, pod.Name, !IsGpuPod(pod), iM, GetCpuComplTime(pod))
					MachinePodMap[pod.Name] = int64(iM)
					PodMachineMap[int64(iM)] = pod.Name
					return pod
				}
			}
		}
	}

	return nil
}*/

// CreateNodeNameToInfoMap obtains a list of pods and pivots that list into a map where the keys are node names
// and the values are the aggregated information for that node.
func CreateNodeNameToInfoMap(pods []*v1.Pod, nodes []*v1.Node) map[string]*framework.NodeInfo {
	NodeNameToInfo := make(map[string]*framework.NodeInfo)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if _, ok := NodeNameToInfo[nodeName]; !ok {
			NodeNameToInfo[nodeName] = framework.NewNodeInfo()
		}
		NodeNameToInfo[nodeName].AddPod(pod)
	}
	imageExistenceMap := createImageExistenceMap(nodes)

	for _, node := range nodes {
		if _, ok := NodeNameToInfo[node.Name]; !ok {
			NodeNameToInfo[node.Name] = framework.NewNodeInfo()
		}
		nodeInfo := NodeNameToInfo[node.Name]
		nodeInfo.SetNode(node)
		nodeInfo.ImageStates = getNodeImageStates(node, imageExistenceMap)
	}
	return NodeNameToInfo
}



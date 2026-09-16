/*
Copyright 2024 The HAMi Authors.

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

package routes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler"
)

const maxRequestSize = 1024 * 1024 // 1MB limit

// 写出的东西抽象成接口（因为要换实现），读入的东西落地成 struct（因为就是数据）；
// struct 太大或要共享就传指针，接口本身已经是引用就别再加指针。
func checkBody(w http.ResponseWriter, r *http.Request) bool {
	if r.Body == nil {
		http.Error(w, "Please send a request body", 400)
		return false
	}
	return true
}

func writeResponse(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(body); err != nil {
		klog.ErrorS(err, "Failed to write response")
	}
}

// 过滤 + 打分阶段的 httphandle
// 真实逻辑在 extenderFilterResult, err = s.Filter(extenderArgs) 中
func PredicateRoute(s *scheduler.Scheduler) httprouter.Handle {
	klog.Infoln("Initializing Predicate Route")
	// httprouter 这个路由库规定，所有注册到它的 handler 必须长这样：
	// type Handle func(http.ResponseWriter, *http.Request, Params)
	// 第三个参数它代表路径参数（URL 路径里的 :id 这种占位符解析出来的键值对）。

	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		klog.V(5).Infoln("Entering Predicate Route handler")
		if !checkBody(w, r) {
			return
		}

		var buf bytes.Buffer
		// Limit the body size to prevent deep nesting/resource exhaustion attacks
		limitedReader := io.LimitReader(r.Body, maxRequestSize)

		//从底层 r 读数据；把读到的数据同时复制一份写到 w 并且把数据返回给调用者。
		// 核心价值：流式处理时既"消费"又"留底"，不需要把全部数据先读进内存。
		// 使用场景：边解析边留原始字节（最常见场景），2. 复制流的同时做副产物。
		// 通用判断标准：只要符合"我要消费一个流，但又想把它经过的全部字节留一份做他用，而且想一次过、不读两遍"——就用 TeeReader。
		body := io.TeeReader(limitedReader, &buf) // buf 当前没有被读区，冗余字段。

		// 输入：kube-scheduler 调 HAMi 的 /filter 时，把"要调度的 Pod + 候选节点列表"打包成 JSON 发过来，HAMi 把它解码进这个变量。
		/*
					type ExtenderArgs struct {
			    		Pod       *v1.Pod      // ← 正在被调度的 Pod（核心：要给它找 GPU）
			    		Nodes     *v1.NodeList //   候选节点（NodeCacheCapable=false 时用）
			    		NodeNames *[]string    //   候选节点名（NodeCacheCapable=true 时用）
					}
		*/
		// 对 HAMi 来说，最关键的是 Pod 字段——里面带着 Pod 申请的 vGPU 资源（hami.io/gpu 等）、设备亲和性注解等。
		// HAMi 拿着这个 Pod 去逐个节点判断"这个 Pod 能不能在这个节点上跑"。用完即弃，请求结束就 GC 掉。
		var extenderArgs extenderv1.ExtenderArgs

		// 输出：HAMi 经过节点过滤后，把结果装进这个变量，最后序列化成 JSON 返回给 kube-scheduler。
		/*
			type ExtenderFilterResult struct {
			    Nodes                      *v1.NodeList  // 通过过滤的节点
			    NodeNames                  *[]string     // 通过的节点名（cache 模式）
			    FailedNodes                FailedNodesMap // 被淘汰的节点 + 失败原因
			    FailedAndUnresolvableNodes  FailedNodesMap // 被淘汰且抢占也救不回来的节点
			    Error                      string        // 整体错误信息
			}
		*/
		var extenderFilterResult *extenderv1.ExtenderFilterResult

		// json.NewDecoder 是流式解码,边从 body 读边解析,不需要先 ReadAll 把整个 body 读进内存。
		if err := json.NewDecoder(body).Decode(&extenderArgs); err != nil {
			klog.ErrorS(err, "Failed to decode extender arguments")
			extenderFilterResult = &extenderv1.ExtenderFilterResult{
				Error: err.Error(),
			}
		} else if extenderArgs.Pod == nil {
			err := fmt.Errorf("extender args missing pod")
			klog.ErrorS(err, "Rejecting filter request with no pod")
			extenderFilterResult = &extenderv1.ExtenderFilterResult{
				Error: err.Error(),
			}
		} else {
			/*
				缓存同步的源头：
				1.K8s informer 本地缓存（节点/Pod 对象本身），这一层同步完，s.synced 并没被置位。也就是说informer 缓存就绪 ≠ 设备视图就绪。
				2.HAMi 自己的设备清单缓存（基于 informer + 节点注解构建）

			*/
			synced := s.WaitForCacheSync(r.Context())
			if !synced {
				// Poll may return false when context is cancelled
				err := fmt.Errorf("context cancelled")
				klog.ErrorS(err, "Cache not synced, cannot proceed with filtering")
				extenderFilterResult = &extenderv1.ExtenderFilterResult{
					Error: err.Error(),
				}
			} else {
				// 实际工作的函数。
				extenderFilterResult, err = s.Filter(extenderArgs)
				if err != nil {
					klog.ErrorS(err, "Filter error for pod", "pod", extenderArgs.Pod.Name)
					extenderFilterResult = &extenderv1.ExtenderFilterResult{
						Error: err.Error(),
					}
				}
			}
		}

		if resultBody, err := json.Marshal(extenderFilterResult); err != nil {
			klog.ErrorS(err, "Failed to marshal extender filter result", "result", extenderFilterResult)
			extenderFilterResult = &extenderv1.ExtenderFilterResult{
				Error: fmt.Sprintf("Failed to marshal extender filter result: %s", err.Error()),
			}
			resultBody, _ = json.Marshal(extenderFilterResult)
			// Note: write error in this fallback path is not explicitly tested
			// as it requires both a marshal failure and a write failure.
			writeResponse(w, http.StatusInternalServerError, resultBody)
		} else {
			writeResponse(w, http.StatusOK, resultBody)
		}
	}
}

func Bind(s *scheduler.Scheduler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		klog.V(5).Infoln("Entering Bind handler")
		var buf bytes.Buffer
		if !checkBody(w, r) {
			return
		}
		// Limit the body size to prevent deep nesting/resource exhaustion attacks
		limitedReader := io.LimitReader(r.Body, maxRequestSize)
		body := io.TeeReader(limitedReader, &buf)

		// 数据流向：
		// kube-scheduler ──HTTP POST /bind──> HAMi ──返回 JSON──> kube-scheduler
		//      (ExtenderBindingArgs)                   (ExtenderBindingResult)

		// kube-scheduler 发给 HAMi 的输入，ExtenderBindingArgs.Node 是 kube-scheduler 已经决定好的目标节点
		var extenderBindingArgs extenderv1.ExtenderBindingArgs

		// HAMi 返回给 kube-scheduler 的输出，ExtenderBindingResult.Error == "" 成功
		var extenderBindingResult *extenderv1.ExtenderBindingResult

		if err := json.NewDecoder(body).Decode(&extenderBindingArgs); err != nil {
			klog.ErrorS(err, "Failed to decode extender binding arguments")
			extenderBindingResult = &extenderv1.ExtenderBindingResult{
				Error: err.Error(),
			}
		} else {
			// 实际工作的函数。
			extenderBindingResult, err = s.Bind(extenderBindingArgs)
			if err != nil {
				klog.ErrorS(err, "Bind error for pod", "pod", extenderBindingArgs.PodName)
				extenderBindingResult = &extenderv1.ExtenderBindingResult{
					Error: err.Error(),
				}
			}
		}

		if response, err := json.Marshal(extenderBindingResult); err != nil {
			klog.ErrorS(err, "Failed to marshal binding result", "result", extenderBindingResult)
			extenderBindingResult = &extenderv1.ExtenderBindingResult{
				Error: fmt.Sprintf("Failed to marshal binding result: %s", err.Error()),
			}
			response, _ := json.Marshal(extenderBindingResult)
			// Note: write error in this fallback path is not explicitly tested
			// as it requires both a marshal failure and a write failure.
			writeResponse(w, http.StatusInternalServerError, response)
		} else {
			klog.V(5).InfoS("Returning bind response", "result", extenderBindingResult)
			writeResponse(w, http.StatusOK, response)
		}
	}
}

/*
用户提交 Pod

	→ API server 收到
	  → 准入控制:调 HAMi /webhook【这里】
	      - 校验参数合法性
	      - MutateAdmission 改容器 spec（注入资源/注解）
	      - 返回 JSON patch
	  → API server 应用 patch,Pod 写入 etcd
	→ Pod 进入调度队列
	  → kube-scheduler 调度
	      → 内置 filter
	      → 调 HAMi /filter（GPU 过滤+选节点）
	      → 选最优节点
	      → 调 HAMi /bind（写 binding）
	  → Pod 绑定到节点
	→ kubelet 起 Pod
	  → device plugin Allocate
*/
func WebHookRoute() httprouter.Handle {
	h, err := scheduler.NewWebHook()
	if err != nil {
		klog.ErrorS(err, "Failed to create new webhook")
	}
	// 如果 h 是 nil,这里不就是 panic 了？
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		klog.V(5).Infof("Handling webhook request on %s", r.URL.Path)
		h.ServeHTTP(w, r)
	}
}

func HealthzRoute() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		klog.V(5).Infoln("Health check endpoint hit")
		w.WriteHeader(http.StatusOK)
	}
}

func ReadyzRoute(s *scheduler.Scheduler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		klog.V(5).Infoln("Readiness check endpoint hit")

		if s.GetLeaderManager().IsLeader() {
			klog.V(5).Infoln("Scheduler extender is leader")
		} else {
			klog.V(3).Infoln("Scheduler extender has not become leader yet")
		}
		w.WriteHeader(http.StatusOK)
	}
}

// NumaRefit handles device-plugin requests to move a pending allocation onto
// kubelet's NUMA-restricted device set. See issue #2080.
func NumaRefit(s *scheduler.Scheduler) httprouter.Handle {
	klog.Infoln("Initializing NumaRefit Route")
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		if !checkBody(w, r) {
			return
		}

		// Limit the body size to prevent deep nesting/resource exhaustion attacks
		body := io.LimitReader(r.Body, maxRequestSize)

		var response device.NumaRefitResponse
		var request device.NumaRefitRequest
		if err := json.NewDecoder(body).Decode(&request); err != nil {
			klog.ErrorS(err, "Failed to decode NUMA refit request")
			response = device.NumaRefitResponse{FailureReason: err.Error()}
		} else if !s.WaitForCacheSync(r.Context()) {
			// Poll may return false when context is cancelled
			err := fmt.Errorf("context cancelled")
			klog.ErrorS(err, "Cache not synced, cannot refit")
			response = device.NumaRefitResponse{FailureReason: err.Error()}
		} else {
			// 实际工作的函数。
			response = s.RefitNumaAllocation(request)
		}

		resultBody, err := json.Marshal(response)
		if err != nil {
			klog.ErrorS(err, "Failed to marshal NUMA refit response", "response", response)
			resultBody, _ = json.Marshal(device.NumaRefitResponse{FailureReason: err.Error()})
			writeResponse(w, http.StatusInternalServerError, resultBody)
			return
		}
		writeResponse(w, http.StatusOK, resultBody)
	}
}

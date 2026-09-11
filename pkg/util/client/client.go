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

package client

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

// buildConfigFromFlags 与 inClusterConfig 是构造 *rest.Config 的两条路径，分别对应
// HAMi "在集群外" 和 "在集群内" 两种运行形态。二者被声明为包级变量而非直接调用，
// 是为了在单测中可替换为 fake（依赖注入），因此本块标注为 "Variables for testing"。
//
// 1) buildConfigFromFlags = clientcmd.BuildConfigFromFlags
//    作用：从 kubeconfig 文件（可带 masterURL 覆盖）解析出 rest.Config。
//    使用场景：开发机、CI 或任何集群外环境运行 HAMi 时，通过 ~/.kube/config
//    （或 KUBECONFIG 指向的文件）连接集群；文件内可含多个 context，由 current-context
//    决定连哪个集群、用哪个用户凭据。
//    注意点：
//      - 文件必须真实存在且可读。这里传入的是显式路径，clientcmd 不会自动兜底 in-cluster；
//        路径不存在会返回 error，由 loadKubeConfig 手动回落到 in-cluster（见下方）。
//      - 凭据可能是 token、client certificate，或云厂商的 exec/auth-provider 插件，
//        依赖运行环境装有对应插件才能生效。
//      - 安全上避免把高权限 kubeconfig 长期留在节点磁盘上。
//
// 2) inClusterConfig = rest.InClusterConfig
//    作用：不依赖任何磁盘文件，直接读取 Pod 内由 kubelet 注入的 ServiceAccount 凭据
//    与 apiserver 地址，构造 rest.Config。
//    使用场景：HAMi 以 Pod/Deployment 形式部署在集群内时（生产形态）。
//      - 凭据：/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt}
//      - 地址：KUBERNETES_SERVICE_HOST / KUBERNETES_SERVICE_PORT 环境变量
//    注意点：
//      - 必须运行在 Pod 中且 ServiceAccount 已挂载，否则报错。
//      - 权限受该 ServiceAccount 的 RBAC（ClusterRole/RoleBinding）约束，需提前授予
//        HAMi 所需的 nodes/pods/leases 等读写权限，否则 list/watch 会失败。
//      - K8s 1.21+ 使用有期限的 projected token，会自动轮换；client-go 传输层会
//        重新读取，无需重启进程。
//
// 二者在 loadKubeConfig() 中按 "先 kubeconfig，失败再 in-cluster" 的顺序兜底，
// 使同一个二进制既能本地调试（连 ~/.kube/config）又能生产部署（走 in-cluster）。
var (
	buildConfigFromFlags = clientcmd.BuildConfigFromFlags
	inClusterConfig      = rest.InClusterConfig
)

type Client struct {
	// Embedded kubernetes.Interface to avoid name conflicts.
	kubernetes.Interface
	config *rest.Config
}

var (
	KubeClient kubernetes.Interface
	once       sync.Once
)

func init() {
	KubeClient = nil
}

// GetClient returns the global Kubernetes client.
func GetClient() kubernetes.Interface {
	return KubeClient
}

// NewClient creates a new Kubernetes client with the given options.
func NewClient(opts ...Option) (*Client, error) {
	restConfig, err := loadKubeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	// Apply WithDefaults option first to set default values.
	WithDefaults()(restConfig)

	// Then apply user-provided options that will override defaults if specified.
	for _, opt := range opts {
		opt(restConfig)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	return &Client{
		Interface: clientset,
		config:    restConfig,
	}, nil
}

// InitGlobalClient initializes the global Kubernetes client with the given options.
func InitGlobalClient(opts ...Option) {
	once.Do(func() {
		client, err := NewClient(opts...)
		if err != nil {
			klog.Fatalf("Failed to initialize global client: %v", err)
		}
		KubeClient = client.Interface
	})
}

// loadKubeConfig 解析 rest.Config，采用 "kubeconfig 文件优先、失败回落 in-cluster" 的兜底策略：
//  1. 取 kubeconfig 路径：优先读 KUBECONFIG 环境变量，为空则回退到 ~/.kube/config。
//  2. 尝试用该路径走 buildConfigFromFlags（集群外路径）。
//  3. 若上一步失败（文件不存在/解析出错），记录日志后改走 inClusterConfig（集群内路径）。
//
// 这样同一个二进制既能本地调试（宿主机上有 ~/.kube/config）又能生产部署（Pod 内无该文件，
// 自动回落到 ServiceAccount 凭据）。注意：传入的是显式路径，clientcmd 自身不会触发 in-cluster
// 兜底，故第 3 步的手动回落是必需的，不可省略。
func loadKubeConfig() (*rest.Config, error) {
	kubeConfigPath := os.Getenv("KUBECONFIG")
	if kubeConfigPath == "" {
		kubeConfigPath = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}

	config, err := buildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		klog.Infof("BuildConfigFromFlags failed for file %s: %v. Using in-cluster config.", kubeConfigPath, err)
		return inClusterConfig()
	}
	return config, nil
}

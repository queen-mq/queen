// queen-operator runs Queen clusters on Kubernetes from QueenCluster objects.
package main

import (
	"flag"
	"net/http"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	queenv1 "github.com/queen-mq/queen/deploy/operator/api/v1alpha1"
	"github.com/queen-mq/queen/deploy/operator/internal/broker"
	"github.com/queen-mq/queen/deploy/operator/internal/controller"
)

var scheme = runtime.NewScheme()

func namespaceOnly(ns string) map[string]cache.Config {
	return map[string]cache.Config{ns: {}}
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(queenv1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr    string
		probeAddr      string
		leaderElection bool
		brokerAccess   string
		namespace      string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to; 0 turns it off.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the health probes bind to.")
	flag.BoolVar(&leaderElection, "leader-elect", false, "Run one active operator among several replicas.")
	flag.StringVar(&brokerAccess, "broker-access", "direct",
		"How the broker port of a pod is reached: direct (the pod's DNS name, which needs a network path from the operator to the pods) or apiserver (the API server's pod proxy, which needs the pods/proxy permission and lets the operator run outside the cluster).")
	flag.StringVar(&namespace, "namespace", "", "Watch QueenClusters in this namespace only; empty watches every namespace.")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	if brokerAccess != "direct" && brokerAccess != "apiserver" {
		log.Info("broker-access must be direct or apiserver", "got", brokerAccess)
		os.Exit(2)
	}

	cfg := ctrl.GetConfigOrDie()
	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElection,
		LeaderElectionID:       "queen-operator.queenmq.com",
	}
	if namespace != "" {
		mgrOpts.Cache.DefaultNamespaces = namespaceOnly(namespace)
	}
	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		log.Error(err, "the manager could not start")
		os.Exit(1)
	}

	var transport func(qc *queenv1.QueenCluster) broker.Transport
	if brokerAccess == "apiserver" {
		clientset, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			log.Error(err, "no Kubernetes client for the pod proxy")
			os.Exit(1)
		}
		transport = func(qc *queenv1.QueenCluster) broker.Transport {
			return &broker.ViaAPIServer{REST: clientset.CoreV1().RESTClient(), Namespace: qc.Namespace, Port: qc.Spec.Port}
		}
	} else {
		httpClient := &http.Client{Timeout: 60 * time.Second}
		transport = func(qc *queenv1.QueenCluster) broker.Transport {
			return &broker.Direct{HTTP: httpClient, Domain: controller.HeadlessDomain(qc), Port: qc.Spec.Port}
		}
	}

	if err := (&controller.QueenClusterReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorderFor("queen-operator"),
		Transport: transport,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "the controller could not be set up")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "readyz")
		os.Exit(1)
	}

	log.Info("starting", "brokerAccess", brokerAccess, "namespace", namespace)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "the manager stopped")
		os.Exit(1)
	}
}

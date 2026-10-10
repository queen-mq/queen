package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"

	"k8s.io/client-go/rest"
)

// Direct reaches a pod at its own DNS name under the headless Service. It
// needs a network path from the operator to the broker port of every pod.
type Direct struct {
	HTTP *http.Client
	// "<cluster>-headless.<namespace>.svc.<cluster domain>"
	Domain string
	Port   int32
}

func (d *Direct) Do(ctx context.Context, pod, method, path string, body []byte) (int, []byte, error) {
	url := fmt.Sprintf("http://%s%s", net.JoinHostPort(pod+"."+d.Domain, strconv.Itoa(int(d.Port))), path)
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := d.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	out, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	return res.StatusCode, out, err
}

// ViaAPIServer reaches a pod through the Kubernetes API server's pod proxy.
// It needs the pods/proxy permission and no network path of its own, which
// is what lets the operator run outside the cluster.
type ViaAPIServer struct {
	REST      rest.Interface
	Namespace string
	Port      int32
}

func (v *ViaAPIServer) Do(ctx context.Context, pod, method, path string, body []byte) (int, []byte, error) {
	req := v.REST.Verb(method).
		Namespace(v.Namespace).
		Resource("pods").
		Name(net.JoinHostPort(pod, strconv.Itoa(int(v.Port)))).
		SubResource("proxy").
		Suffix(path)
	if body != nil {
		req = req.SetHeader("Content-Type", "application/json").Body(body)
	}
	res := req.Do(ctx)
	var status int
	res.StatusCode(&status)
	out, err := res.Raw()
	if status == 0 {
		return 0, nil, err
	}
	// A failure of the proxy itself (the pod does not answer, the name does
	// not exist) comes back as a Kubernetes Status object, not from the broker.
	var st struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	}
	if status >= 400 && json.Unmarshal(out, &st) == nil && st.Kind == "Status" {
		return 0, nil, fmt.Errorf("the API server could not reach %s: %s", pod, st.Message)
	}
	// The broker answered: its status and body are the result, whatever
	// client-go makes of a non-2xx.
	return status, out, nil
}

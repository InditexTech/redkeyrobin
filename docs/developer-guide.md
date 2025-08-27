<!--
SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL S.A. (INDITEX S.A.)

SPDX-License-Identifier: Apache-2.0
-->

# Developer Guide

Quickly provision RedKey Robin in Kubernetes.

## Local development and testing

A *local K8s cluster* can be used to deploy the RedKey Operator from a pre-built image or directly compiling from the source code.

This will be helpfull to develop and test new features, fix bugs, and test releases locally.

As we will see below, you can use the `make` command to deploy the different components, as well as to deploy a sample RedKey Cluster , or use the scripts that automate the basic workflows.

## Development profiles

Three **Deployment Profiles** have been defined. Basically, these profiles determine which image will be used to deploy the RedKey Operator pod. These are the 3 Deployment Profiles and the default images used to create the RedKey Operator pod:

| Profile | Image used to create the RedKey Operator pod | Purpose |
|---------|---------------------------------------------|---------|
| debug | delve:1.24.0 | Debug code from your IDE using Delve |
| dev | redis-robin:0.1.0-dev | Test a locally built (from source code) release | 
| pro | redis-robin:0.1.0 | Test a released version |

## Create your Kubernetes cluster

You can deploy your own Kubernetes cluster locally to develop RedKey Operator with the tool of your choice (Docker Desktop, Rancher Desktop, K3D, Kind,...).

We recommend you to use Kind. To start a local registry and a K8S cluster with Kind you can use the following script (from Kind official doc page):

```bash
#!/bin/sh
set -o errexit

# 1. Create registry container unless it already exists
reg_name='kind-registry'
reg_port='5001'
if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
  docker run \
    -d --restart=always -p "127.0.0.1:${reg_port}:5000" --name "${reg_name}" \
    registry:2
fi

# 2. Create kind cluster with containerd registry config dir enabled
# TODO: kind will eventually enable this by default and this patch will
# be unnecessary.
#
# See:
# https://github.com/kubernetes-sigs/kind/issues/2875
# https://github.com/containerd/containerd/blob/main/docs/cri/config.md#registry-configuration
# See: https://github.com/containerd/containerd/blob/main/docs/hosts.md
cat <<EOF | kind create cluster --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry]
    config_path = "/etc/containerd/certs.d"
EOF

# 3. Add the registry config to the nodes
#
# This is necessary because localhost resolves to loopback addresses that are
# network-namespace local.
# In other words: localhost in the container is not localhost on the host.
#
# We want a consistent name that works from both ends, so we tell containerd to
# alias localhost:${reg_port} to the registry container when pulling images
REGISTRY_DIR="/etc/containerd/certs.d/localhost:${reg_port}"
for node in $(kind get nodes); do
  docker exec "${node}" mkdir -p "${REGISTRY_DIR}"
  cat <<EOF | docker exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
[host."http://${reg_name}:5000"]
EOF
done

# 4. Connect the registry to the cluster network if not already connected
# This allows kind to bootstrap the network but ensures they're on the same network
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = 'null' ]; then
  docker network connect "kind" "${reg_name}"
fi

# 5. Document the local registry
# https://github.com/kubernetes/enhancements/tree/master/keps/sig-cluster-lifecycle/generic/1755-communicating-a-local-registry
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF
#!/bin/sh
set -o errexit
```


## RedKey Robin

### Deploy RedKey Robin from a custom image

Once your K8s cluster and registry are ready to work with, you need to make available the image you want to use to deploy RedKey Robin.

Your cluster must be configured to be able to access your local registry.

We provide and easy way to build and push the images for `dev` and `debug` profiles:

- `make dev-docker-build`: builds an image containing the RedKey Robin built from the source code. This image is published in Docker local registry.
- `make dev-docker-push`: pushes the image built with the command above to the corresponding registry.
- `make debug-docker`: builds an image that will allow us to create an *empty* pod as the redkey operator to which we will copy the manager binary and run it, as we'll explain later.
- `make debug-docker-push`: pushes the image built with the command above to he corresponding registry.

**To test a released RedKey Robin version you'll have to manually pull the image, tag and push to your local registry.**

The image names used by default by each profile (shown in the table above) can be overwritten using the environment variables:

- `IMG`: overwrittes the image name when using `pro` profile.
- `IMG_DEV`: overwrittes the image name when using `dev` profile.
- `IMG_DEBUG`: overwrittes the image name when using `debug` profile.

E.G. these are the commands to build and push the image for `debug` profile:

```
make debug-docker-build IMG_DEBUG=localhost:5001/redis-robin:delve
make debug-docker-push IMG_DEBUG=localhost:5001/redis-robin:delve
```

E.G. the commands when using `dev` profile:

```
make debug-docker-build-robin IMG_DEV_ROBIN=localhost:5001/redis-robin:0.1.0
make debug-docker-push-robin IMG_DEV_ROBIN=localhost:5001/redis-robin:0.1.0
```

Once the RedKey Robin is available in your local registry, you can follow these steps to deploy it into you K8s cluster:

1. Install the RedKey Operator. Please refer to the [RedKey Operator](https://github.com/InditexTech/redkeyoperator/) to know how to deploy the RedKey Operator.

2. Create a RedKey Cluster  setting `spec.robin.template.spec.containers[0].image` to `$IMG_ROBIN`, `$IMG_DEV_ROBIN` or `$IMG_DEBUG` depending on the profile you want to test. You can use the RedKey Cluster  sample in `config/samples/redis_v1_redkeycluster.yml`



### Debuging RedKey Robin

If you followed the steps described above to deploy the RedKey Robin using the `debug` profile you'll have a RedKey Cluster  with a RedKey Robin deployed.

This pod is created using a `golang` image with `Delve` installed on it. This will allow us to easily debug the robin code following these steps:

1. Build the robin binary file from the source code.
2. Copy the robin binary file to the `<RedKeyClusterName>`-robin pod.
3. Run the robin binary inside the `<RedKeyClusterName>`-robin pod, using Delve to allow remote debugging connections.
4. Port forward `40002` port to be able to connect to the exposed port in the `<RedKeyClusterName>`-robin pod.
5. Connect from the IDE of your choice to remote debugging

To follow the 3 first steps simply execute:

```
make debug-robin
```

The robin will the be running in your `<RedKeyClusterName>`-robin pod and you'll see pod's standard output printing in your terminal. The robin logs will then be streamed to the terminal.

To enable the port forwarding:

```
make port-forward-robin
```

You can now attach your local debug from the IDE of your choice to the debug session in the `<RedKeyClusterName>`-robin pod. As an example, you'll find the configuration needed to launch the debug from VSCode here:

```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Connect to robin",
            "type": "go",
            "request": "attach",
            "mode": "remote",
            "remotePath": "${workspaceFolder}",
            "port": 40002,
            "host": "127.0.0.1",
            "trace": "verbose",
            "env": {
                "WATCH_NAMESPACE": "default",
                "KUBERNETES_CONFIG":  "${HOME}/.kube/config",
            }
        }
    ]
}
```

Customize the configuration to use your kubeconfig and your namespace if needed.


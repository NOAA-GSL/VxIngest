# Kubernetes VxIngest Jobs

This directory contains Kubernetes manifests related to running VxIngest jobs in-cluster.

The current branch ships:

- a standalone ingest job manifest
- a pipeline orchestrator job manifest
- the ConfigMaps and RBAC needed by that orchestrator

The orchestrator script still contains embedded definitions for downstream import and meta-update child jobs, but those downstream runtimes are not maintained in this branch.

## What Is Included

- `namespace.yaml`: namespace used by all resources
- `secret-credentials.template.yaml`: Couchbase credentials secret template
- `secret-cacert.template.yaml`: optional Capella CA cert secret template
- `configmap-job-params.yaml`: orchestrator runtime parameters, including `JOBIDS`
- `configmap-job-script.yaml`: orchestrator shell script with embedded child job templates
- `job-ingest.yaml`: standalone one-time ingest job
- `job-orchestrator.yaml`: one-time orchestrator job
- `sa-orchestrator.yaml`, `role-orchestrator.yaml`, `rolebinding-orchestrator.yaml`: RBAC for the orchestrator

## Prerequisites

1. Push the ingest image to a registry your cluster can pull from.
2. Fill in `secret-credentials.template.yaml` with valid values.
3. If using Capella (`cloud.couchbase.com`), fill in `secret-cacert.template.yaml`.
4. Create or supply PVCs named `vxingest-data-pvc` and `vxingest-public-pvc`.
5. If you plan to run the orchestrator, review `configmap-job-script.yaml` and confirm the referenced downstream import and meta-update images exist in your environment.

## Create or update runtime secrets

Use your local files to create/update the Kubernetes secret consumed by the Job/CronJob.
Keep real credentials and private keys in your home directory, not in this repository.
If your Couchbase HTTPS endpoint uses an internal or private CA, also provide a PEM bundle for `cb-ca.pem`. THIS SHOULD NOT BE NECESSARY FOR CAPELLA, but it is for the internal cluster.

Option A: use the helper script:

```console
./kubernetes/create-secrets-from-home.sh
```

Optional overrides:

```console
NAMESPACE=vxingest-dev KUBECONFIG_PATH=${HOME}/.kube/development.yaml ./kubernetes/create-secrets-from-home.sh
```

Optional Couchbase CA override:

```console
CB_CA_FILE=${HOME}/.ssh/cb-ca.pem NAMESPACE=vxingest-dev KUBECONFIG_PATH=${HOME}/.kube/development.yaml ./kubernetes/create-secrets-from-home.sh
```

Option B: run kubectl directly:

```console
kubectl --kubeconfig=${HOME}/.kube/development.yaml --namespace vxingest-dev create secret generic vxingest-credentials \
  --from-file=credentials.yaml=${HOME}/credentials.yaml \
  --dry-run=client -o yaml | kubectl --kubeconfig=${HOME}/.kube/development.yaml --namespace vxingest-dev apply -f -

kubectl --kubeconfig=${HOME}/.kube/development.yaml --namespace vxingest-dev create secret generic vxingest-cacert \
  --from-file=cacert.pem=${HOME}/capella-root-certificate.pem \
  --dry-run=client -o yaml | kubectl --kubeconfig=${HOME}/.kube/development.yaml --namespace vxingest-dev apply -f -
```

### imagePullSecret for NOAA_GSL ghcr.io

In GSL, we need to add a secret to the intended namespace so we can pull from GHCR.

```console
 kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev create secret docker-registry vxingest-ghcr \
 --docker-server=ghcr.io \
 --docker-username=<your username> \
 --docker-password=<PAT with read:packages permission and granted SSO access to the GSL GitHub org>
```

## Apply Resources

Apply the resources you need explicitly. The ingest job and orchestrator job are independent entry points.

Standalone ingest job:

```bash
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/namespace.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/secret-credentials.template.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/secret-cacert.template.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/job-ingest.yaml
```

Pipeline orchestrator:

```bash
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/namespace.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/configmap-job-params.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/configmap-job-script.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/sa-orchestrator.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/role-orchestrator.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/rolebinding-orchestrator.yaml
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -f kubernetes/job-orchestrator.yaml
```

## Run Jobs

Run the standalone ingest job or the orchestrator job independently:

```bash
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev delete job vxingest-ingest --ignore-not-found
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev apply -f kubernetes/job-ingest.yaml

kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev delete job vxingest-pipeline-orchestrator --ignore-not-found
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev apply -f kubernetes/job-orchestrator.yaml
```

## Inspect Status

```bash
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev get jobs
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev get pods
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev logs job/vxingest-ingest
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest-dev logs job/vxingest-pipeline-orchestrator
```

## Notes

- `job-ingest.yaml` expects PVCs named `vxingest-data-pvc` and `vxingest-public-pvc` to exist already.
- The orchestrator script reads `JOBIDS` from `configmap-job-params.yaml` and creates one ingest/import pair per configured line.
- The CA cert secret is optional in manifests. Keep it populated for Capella deployments.
- All jobs are configured with `restartPolicy: Never` and `backoffLimit: 0` for one-shot behavior.

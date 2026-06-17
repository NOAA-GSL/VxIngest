# Kubernetes One-Time VxIngest related Jobs

This directory contains Kubernetes manifests for running VxIngest services as one-time jobs:

- ingest
- import
- meta-update

## What Is Included

- `namespace.yaml`: namespace used by all resources
- `secret-credentials.template.yaml`: Couchbase credentials secret template
- `secret-cacert.template.yaml`: optional Capella CA cert secret template
- `pvc-data.yaml`: shared data PVC for ingest/import/meta-update
- `pvc-public.yaml`: public data PVC mounted at `/public` for ingest
- `job-ingest.yaml`: one-time ingest job
- `job-import.yaml`: one-time import job
- `job-meta-update.yaml`: one-time meta-update job
- `kustomization.yaml`: bundle of all resources

## Prerequisites

1. Push the ingest/import images to a registry your cluster can pull from.
2. Update image names/tags in `job-ingest.yaml`, `job-import.yaml`, and `job-meta-update.yaml`.
3. Fill in `secret-credentials.template.yaml` with valid values.
4. If using Capella (`cloud.couchbase.com`), fill in `secret-cacert.template.yaml`.
5. Update PVC size/storage class as needed for your cluster.

## Create or update runtime secrets

Use your local files to create/update the Kubernetes secret consumed by the Job/CronJob.
Keep real credentials and private keys in your home directory, not in this repository.
If your Couchbase HTTPS endpoint uses an internal or private CA, also provide a PEM bundle for `cb-ca.pem`. THIS SHOULD NOT BE NECESSARY FOR CAPELLA, but it is for the internal cluster.

Option A: use the helper script:

```console
./kubernetes/create-secret-from-home.sh
```

Optional overrides:

```console
NAMESPACE=vxingest-dev KUBECONFIG_PATH=${HOME}/.kube/development.yaml ./kubernetes/create-secret-from-home.sh
```

Optional Couchbase CA override:

```console
CB_CA_FILE=${HOME}/.ssh/cb-ca.pem NAMESPACE=vxingest-dev KUBECONFIG_PATH=${HOME}/.kube/development.yaml ./kubernetes/create-secret-from-home.sh
```

Option B: run kubectl directly:

```console
kubectl --kubeconfig=${HOME}/.kube/development.yaml --namespace vxingest-dev create secret generic mats-metadata-secrets \
  --from-file=credentials.yaml=${HOME}/credentials.yaml \
  --from-file=deploy_key.pem=${HOME}/.ssh/id_ed25519 \
  --from-file=known_hosts=${HOME}/.ssh/known_hosts \
  --from-file=cb-ca.pem=${HOME}/.ssh/cb-ca.pem \
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

## Apply Base Resources

Apply everything:

```bash
kubectl --kubeconfig=${HOME}/.kube/development.yaml apply -k kubernetes
```

## Run Jobs

Run each service independently (delete and recreate to run again):

```bash
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest delete job vxingest-ingest --ignore-not-found
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest apply -f kubernetes/job-ingest.yaml

kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest delete job vxingest-import --ignore-not-found
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest apply -f kubernetes/job-import.yaml

kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest delete job vxingest-meta-update --ignore-not-found
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest apply -f kubernetes/job-meta-update.yaml
```

## Inspect Status

```bash
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest get jobs
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest get pods
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest logs job/vxingest-ingest
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest logs job/vxingest-import
kubectl --kubeconfig=${HOME}/.kube/development.yaml -n vxingest logs job/vxingest-meta-update
```

## Notes

- The import job creates required directories (`logs`, `xfer`, `temp_tar`) before running.
- The CA cert secret is optional in manifests. Keep it populated for Capella deployments.
- All jobs are configured with `restartPolicy: Never` and `backoffLimit: 0` for one-shot behavior.

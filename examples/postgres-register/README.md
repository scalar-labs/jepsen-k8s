# postgres-register

A small Jepsen register test for PostgreSQL on Kubernetes.

This example uses `jepsen-k8s` for the Kubernetes-specific parts:

- install PostgreSQL with Helm
- wait for PostgreSQL pods to become Ready
- expose PostgreSQL through a `LoadBalancer` service
- collect pod logs at teardown

## Usage

Install the local library first from the repository root:

```sh
lein install
```

Then run this example:

```sh
cd examples/postgres-register
lein run -- test --no-ssh --time-limit 30 \
  --kube-context kind-jepsen \
  --namespace jepsen-postgres \
  --release jepsen-postgres
```

Enable Chaos Mesh nemeses with `--nemesis`. The flag may be repeated:

```sh
lein run -- test --no-ssh --time-limit 60 \
  --kube-context kind-jepsen \
  --namespace jepsen-postgres \
  --release jepsen-postgres \
  --nemesis kill \
  --nemesis clock \
  --nemesis-interval 10
```

Supported values are `pause`, `kill`, `partition`, `packet`, and `clock`.
When `--nemesis` is present, this example installs Chaos Mesh during DB setup.

The example sets PostgreSQL's service type to `LoadBalancer` and waits for an
address from MetalLB before connecting.

By default the example uses Bitnami's PostgreSQL chart:

```text
oci://registry-1.docker.io/bitnamicharts/postgresql
```

If PostgreSQL is exposed some other way, pass a JDBC URL:

```sh
lein run -- test --no-ssh \
  --jdbc-url jdbc:postgresql://postgres.example.com:5432/jepsen
```

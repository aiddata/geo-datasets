## geodata helm chart

1. Storage persistence is achieved in Kubernetes through the use of [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).
   These represent physical media, whether on a node's filesystem or on a cloud storage service.
   In practice, a persistent volume will be set up for us on the Nova cluster.
   To run this locally, however, you'll need to make one yourself.
   Do this by applying `pv.yaml` to your cluster:
   ```shell
   kubectl apply -f pv.yaml
   ```

2. Persistent Volumes are allocated in Kubernetes using Persistent Volume Claims.
   These Claims allow you to divide up large Persistent Volumes to be used by different resources on the cluster.
   This will also likely be managed for us on Nova.
   I've written `pvc.yaml` for local development:
   ```shell
   kubectl apply -f pvc.yaml
   ```

3. Pods running within Kubernetes can be assigned Service Accounts, which can then be given role-based access to other resources in the cluster.
   Prefect agent pods need a variety of permissions in order to launch and monitor jobs for each deployment run.
   `serviceaccount.yaml` includes definitions for a Service Account, and a few Roles and Role Bindings for it.
   ```shell
   kubectl apply -f serviceaccount.yaml
   ```

4. Now it's time to deploy our container
   ```shell
   conda activate geodata38
   # review generate_manifest.py and change any necessary variables
   python generate_manifest.py > orion.yaml
   kubectl apply -f orion.yaml
   ```

   The deployment should now be spinning up in minikube!
   Check on it by running:
   ```shell
   kubectl describe deployment prefect-agent
   ```

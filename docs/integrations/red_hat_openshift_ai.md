---
title: Red Hat OpenShift AI
parent: Integrations
redirect_from: 
---

# Using lakeFS with Red Hat OpenShift AI
Red Hat® OpenShift® is an enterprise-ready Kubernetes container platform with full-stack automated operations to manage hybrid cloud, multi-cloud, and edge deployments. OpenShift includes an enterprise-grade Linux operating system, container runtime, networking, monitoring, registry, and authentication and authorization solutions.

[Red Hat® OpenShift® AI](https://www.redhat.com/en/technologies/cloud-computing/openshift/openshift-ai) is a flexible, scalable artificial intelligence (AI) and machine learning (ML) platform that enables enterprises to create and deliver AI-enabled applications at scale across hybrid cloud environments. Built using open source technologies, OpenShift AI provides trusted, operationally consistent capabilities for teams to experiment, serve models, and deliver innovative apps.

OpenShift AI and lakeFS can be deployed in OpenShift cluster in 3 different architectures:
1. OpenShift AI, lakeFS and object storage are delpoyed in OpenShift cluster
2. OpenShift AI and lakeFS are deployed in OpenShift cluster while object storage is external
3. OpenShift AI is deployed in OpenShift cluster while lakeFS and object storage are external

<img src="{{ site.baseurl }}/assets/img/red-hat/OpenShiftDeploymentArchitecture.png" alt="OpenShift AI and lakeFS Deployment Architecture" width="100%" height="100%" />

Refer to an example in [lakeFS-samples](https://github.com/treeverse/lakeFS-samples/tree/main/01_standalone_examples/red-hat-openshift-ai) to deploy lakeFS, MinIO and OpenShift AI tutorial ([Fraud Detection demo](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/2-latest/html/openshift_ai_tutorial_-_fraud_detection_example/index)) in OpenShift cluster. Fraud detection demo is a step-by-step guide for using OpenShift AI to train an example model in JupyterLab, deploy the model, and refine the model by using automated pipelines.

In this example, OpenShift AI is configured to connect over S3 interace to lakeFS, which will version the data in a backend MinIO instance. This is the architecture to run Fraud Detection demo with or without lakeFS:
<img src="{{ site.baseurl }}/assets/img/red-hat/OpenShiftAIDemoArchitecture.png" alt="OpenShift AI and lakeFS Deployment Architecture for the demo" width="100%" height="100%" />

lakeFS-samples also includes multiple Helm chart examples to deploy lakeFS and MinIO in different scenarios:
1. [lakefs-local.yaml](https://github.com/treeverse/lakeFS-samples/blob/main/01_standalone_examples/red-hat-openshift-ai/cluster-configuration/lakefs-local.yaml): Bring up lakeFS using local object storage. This would be useful for a quick demo where MinIO is not included.
2. [lakefs-minio.yaml](https://github.com/treeverse/lakeFS-samples/blob/main/01_standalone_examples/red-hat-openshift-ai/cluster-configuration/lakefs-minio.yaml): Bring up lakeFS configured to use MinIO as backend object storage. This will be used in the lakeFS demo.
3. [minio-direct.yaml](https://github.com/treeverse/lakeFS-samples/blob/main/01_standalone_examples/red-hat-openshift-ai/cluster-configuration/minio-direct.yaml): This file would only be used if lakeFS is not in the picture and OpenShift AI will communicate directly with MinIO. It will bring up MinIO as it is in the default Fraud Detection demo, complete with configuring MinIO storage buckets and the OpenShift AI data connections. It may serve useful in debugging an issue.
4. [minio-via-lakefs.yaml](https://github.com/treeverse/lakeFS-samples/blob/main/01_standalone_examples/red-hat-openshift-ai/cluster-configuration/minio-via-lakefs.yaml): Bring up MinIO for the modified Fraud Detection demo that includes lakeFS, complete with configuring MinIO storage buckets, but do NOT configure the OpenShift AI data connections. This will be used in the lakeFS demo.

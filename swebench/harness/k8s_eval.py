"""
Kubernetes evaluation for SWE-bench instances.
This module provides functionality to run SWE-bench evaluations on Kubernetes pods
instead of Docker containers.
"""
from __future__ import annotations

import json
import os
import time
import traceback
from pathlib import Path
from typing import List, Tuple

import kubernetes
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from swebench.harness.constants import (
    APPLY_PATCH_FAIL,
    APPLY_PATCH_PASS,
    DOCKER_PATCH,
    DOCKER_WORKDIR,
    KEY_MODEL,
    KEY_PREDICTION,
    LOG_REPORT,
    LOG_INSTANCE,
    LOG_TEST_OUTPUT,
    RUN_EVALUATION_LOG_DIR,
)
from swebench.harness.docker_build import setup_logger, close_logger
from swebench.harness.grading import get_eval_report
from swebench.harness.test_spec.test_spec import TestSpec
from swebench.harness.utils import EvaluationError, run_threadpool

AVOID_NODES: list[str] = [
    "k8s-chase-ci-03.calit2.optiputer.net",
    "k8s-chase-ci-02.calit2.optiputer.net",
    "ry-gpu-15.sdsc.optiputer.net",
    "rci-tide-cpu-02.sdsu.edu",
    "gp-engine.hpc.okstate.edu",
    "k8s-gpu-01.calit2.optiputer.net",
    "dtn-gpu2.kreonet.net"
    ]
AVOID_NODES += [f"k8s-haosu-{i:02d}.sdsc.optiputer.net" for i in range(1, 40)]


GIT_APPLY_CMDS = [
    "git apply --verbose",
    "git apply --verbose --reject",
    "patch --batch --fuzz=5 -p1 -i",
]


def validate_k8s_credentials():
    """
    Validate that Kubernetes credentials are available and working.
    """
    try:
        config.load_config()
        v1 = client.CoreV1Api()
        # Test connection by listing namespaces
        v1.list_namespace(limit=1)
        print("✓ Kubernetes credentials validated")
    except Exception as e:
        raise RuntimeError(f"Failed to validate Kubernetes credentials: {e}")


def create_pod_spec(
    test_spec: TestSpec,
    instance_id: str,
    namespace: str,
    run_id: str,
    naming_prefix: str = "bking2"
) -> client.V1Pod:
    """
    Create a Kubernetes pod specification for running a SWE-bench instance.
    
    Args:
        test_spec: TestSpec instance
        instance_id: Instance ID for naming
        namespace: Kubernetes namespace
        run_id: Run ID for labeling
        naming_prefix: if provided, prefixes pod and other resource names with `<prefix>-`
        
    Returns:
        V1Pod specification
    """
    app_name = "swebench"
    naming_prefix = os.environ.get("SWEBENCH_POD_NAME_PREFIX", naming_prefix)
    if naming_prefix:
        app_name = naming_prefix + "-" + app_name
    pod_name = f"{app_name}-{instance_id.lower()}-{run_id}".replace('_', '-')

    
    # Use the instance image key as the image name
    # This assumes images are available on Docker Hub
    image_name = test_spec.instance_image_key
    node_affinity = None
    if AVOID_NODES:
        node_affinity = client.V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
                node_selector_terms=[
                    client.V1NodeSelectorTerm(
                        match_expressions=[
                            client.V1NodeSelectorRequirement(
                                key="kubernetes.io/hostname",
                                operator="NotIn",
                                values=AVOID_NODES,
                            )
                        ]
                    )
                ]
            )
        )
    pod_spec = client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(
            name=pod_name,
            namespace=namespace,
            labels={
                "app": app_name,
                "run-id": run_id,
                "instance-id": instance_id,
            }
        ),
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(
                    name="swebench-container",
                    image=image_name,
                    command=["/bin/bash", "-c", "tail -f /dev/null"],
                    working_dir=DOCKER_WORKDIR,
                    image_pull_policy="Always",  # Always pull to ensure latest
                    resources=client.V1ResourceRequirements(
                        requests={"cpu": "1", "memory": "2Gi"},
                        limits={"cpu": "1", "memory": "2Gi"},
                    ),
                )
            ],
            affinity=client.V1Affinity(
                node_affinity=node_affinity
            )
        )
    )
    
    return pod_spec


def wait_for_pod_ready(v1: client.CoreV1Api, pod_name: str, namespace: str, timeout: int = 480) -> bool:
    """
    Wait for a pod to be ready.
    
    Args:
        v1: Kubernetes CoreV1Api client
        pod_name: Name of the pod
        namespace: Kubernetes namespace
        timeout: Timeout in seconds
        
    Returns:
        True if pod is ready, False if timeout or failed
    """
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)
            
            # Check for terminal failure states first
            if pod.status.phase in ["Failed", "Succeeded"]:
                print(f"Pod {pod_name} reached terminal state: {pod.status.phase}")
                return False
            
            # Check container statuses for image pull failures
            if pod.status.container_statuses:
                for container in pod.status.container_statuses:
                    if container.state and container.state.waiting:
                        reason = container.state.waiting.reason
                        if reason in ["ImagePullBackOff", "ErrImagePull", "InvalidImageName"]:
                            print(f"Pod {pod_name} failed with image issue: {reason} - {container.state.waiting.message}")
                            return False
                        elif reason in ["CrashLoopBackOff", "RunContainerError"]:
                            print(f"Pod {pod_name} failed with container issue: {reason} - {container.state.waiting.message}")
                            return False
            
            # Check if pod is running and all containers are ready
            if pod.status.phase == "Running":
                if pod.status.container_statuses:
                    all_ready = all(
                        container.ready for container in pod.status.container_statuses
                    )
                    if all_ready:
                        return True
                
        except ApiException as e:
            if e.status == 404:
                # Pod not found yet
                pass
            else:
                raise
        
        time.sleep(2)
    
    print(f"Pod {pod_name} timed out after {timeout} seconds")
    return False


def copy_file_to_pod(
    pod_name: str,
    namespace: str,
    local_file_path: str | Path,
    dest_path: str,
) -> bool:
    """Copy file to pod using kubectl cp."""
    try:
        import subprocess
        if isinstance(local_file_path, Path):
            local_file_path = local_file_path.as_posix()
        
        cmd = [
            "kubectl", "cp",
            local_file_path,
            f"{namespace}/{pod_name}:{dest_path}"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            return True
        else:
            print(f"kubectl cp failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error copying file to pod: {e}")
        return False


def exec_command_in_pod(
    v1: client.CoreV1Api,
    pod_name: str,
    namespace: str,
    command: List[str],
    timeout: int = None,
) -> Tuple[str, int]:
    """
    Execute a command in the pod and return output and exit code.
    
    Args:
        v1: Kubernetes CoreV1Api client
        pod_name: Name of the pod
        namespace: Kubernetes namespace
        command: Command to execute
        timeout: Timeout in seconds (not strictly enforced)
        
    Returns:
        Tuple of (output, exit_code)
    """
    try:
        # Execute command
        resp = kubernetes.stream.stream(
            v1.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
        
        # For now, assume success if no exception
        # In a more robust implementation, we'd capture the actual exit code
        return resp, 0
        
    except Exception as e:
        return str(e), 1


def run_instance_k8s(
    test_spec: TestSpec,
    pred: dict,
    run_id: str,
    k8s_namespace: str,
    timeout: int | None = None,
    rewrite_reports: bool = False,
) -> Tuple[str, dict] | None:
    """
    Run a single instance evaluation on Kubernetes.
    
    Args:
        test_spec: TestSpec instance
        pred: Prediction dictionary with model_name_or_path, model_patch, instance_id
        run_id: Run ID
        k8s_namespace: Kubernetes namespace
        timeout: Timeout for running tests
        rewrite_reports: True if eval run is just to reformat existing report
        
    Returns:
        Tuple of (instance_id, report) or None on failure
    """
    # Set up logging directory
    instance_id = test_spec.instance_id
    model_name_or_path = pred.get(KEY_MODEL, "None").replace("/", "__")
    log_dir = RUN_EVALUATION_LOG_DIR / run_id / model_name_or_path / instance_id

    # Set up report file
    report_path = log_dir / LOG_REPORT
    if rewrite_reports:
        test_output_path = log_dir / LOG_TEST_OUTPUT
        if not test_output_path.exists():
            raise ValueError(f"Test output file {test_output_path} does not exist")
        report = get_eval_report(
            test_spec=test_spec,
            prediction=pred,
            test_log_path=test_output_path,
            include_tests_status=True,
        )
        # Write report to report.json
        with open(report_path, "w") as f:
            f.write(json.dumps(report, indent=4))
        return instance_id, report
    
    if report_path.exists():
        return instance_id, json.loads(report_path.read_text())

    # Set up logger
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / LOG_INSTANCE
    logger = setup_logger(instance_id, log_file)

    # Initialize Kubernetes client
    try:
        config.load_config()
        v1 = client.CoreV1Api()
    except Exception as e:
        logger.error(f"Failed to initialize Kubernetes client: {e}")
        return None

    pod_name = None
    try:
        # Create pod
        pod_spec = create_pod_spec(test_spec, instance_id, k8s_namespace, run_id)
        pod_name = pod_spec.metadata.name
        
        logger.info(f"Creating pod {pod_name} for {instance_id}")
        v1.create_namespaced_pod(namespace=k8s_namespace, body=pod_spec)
        
        # Wait for pod to be ready
        logger.info(f"Waiting for pod {pod_name} to be ready...")
        timeout: int = 900  # 15 minutes
        if not wait_for_pod_ready(v1, pod_name, k8s_namespace, timeout=timeout):
            raise EvaluationError(
                instance_id,
                f"Pod {pod_name} failed to become ready within timeout {timeout}",
                logger,
            )
        
        logger.info(f"Pod {pod_name} is ready")
        
        # Copy patch file to pod
        patch_content = pred[KEY_PREDICTION] or ""
        patch_file = Path(log_dir / "patch.diff")
        patch_file.write_text(patch_content)
        
        logger.info(f"Copying patch to pod {pod_name}...")
        if not copy_file_to_pod(pod_name, k8s_namespace, patch_file, DOCKER_PATCH):
            raise EvaluationError(
                instance_id,
                f"Failed to copy patch to pod {pod_name}",
                logger,
            )

        # Apply patch
        applied_patch = False
        for git_apply_cmd in GIT_APPLY_CMDS:
            command = ["/bin/bash", "-c", f"cd {DOCKER_WORKDIR} && {git_apply_cmd} {DOCKER_PATCH}"]
            output, exit_code = exec_command_in_pod(v1, pod_name, k8s_namespace, command)
            
            if exit_code == 0:
                logger.info(f"{APPLY_PATCH_PASS}:\n{output}")
                applied_patch = True
                break
            else:
                logger.info(f"Failed to apply patch: {git_apply_cmd}")
        
        if not applied_patch:
            logger.info(f"{APPLY_PATCH_FAIL}:\n{output}")
            raise EvaluationError(
                instance_id,
                f"{APPLY_PATCH_FAIL}:\n{output}",
                logger,
            )

        # Get git diff before running eval script
        command = ["/bin/bash", "-c", f"cd {DOCKER_WORKDIR} && git -c core.fileMode=false diff"]
        git_diff_output_before, _ = exec_command_in_pod(v1, pod_name, k8s_namespace, command)
        logger.info(f"Git diff before:\n{git_diff_output_before}")

        # Copy eval script to pod
        eval_content = test_spec.eval_script
        eval_file = Path(log_dir / "eval.sh")
        eval_file.write_text(eval_content)
        
        logger.info(f"Copying eval script to pod {pod_name}...")
        if not copy_file_to_pod(pod_name, k8s_namespace, str(eval_file), "/eval.sh"):
            raise EvaluationError(
                instance_id,
                f"Failed to copy eval script to pod {pod_name}",
                logger,
            )
        # Hack for pvlib (downgrade do numpy 1.0)
        if 'pvlib' in instance_id:
            logger.warning("Hacking numpy version for pvlib problems")
            install_commands = [
                "source /opt/miniconda3/bin/activate",
                f"conda activate testbed",
                f"pip uninstall numpy -y && pip install numpy<2.0",
            ]
            cmd_output, exit_code = exec_command_in_pod(v1, pod_name, k8s_namespace, "\n".join(install_commands), timeout=300)
        # Run eval script
        start_time = time.time()
        command = ["/bin/bash", "/eval.sh"]
        test_output, exit_code = exec_command_in_pod(v1, pod_name, k8s_namespace, command, timeout)
        total_runtime = time.time() - start_time
        
        test_output_path = log_dir / LOG_TEST_OUTPUT
        logger.info(f"Test runtime: {total_runtime:_.2f} seconds")
        with open(test_output_path, "w") as f:
            f.write(test_output)
            logger.info(f"Test output for {instance_id} written to {test_output_path}")

        # Get git diff after running eval script
        command = ["/bin/bash", "-c", f"cd {DOCKER_WORKDIR} && git -c core.fileMode=false diff"]
        git_diff_output_after, _ = exec_command_in_pod(v1, pod_name, k8s_namespace, command)
        logger.info(f"Git diff after:\n{git_diff_output_after}")
        if git_diff_output_after != git_diff_output_before:
            logger.info("Git diff changed after running eval script")

        # Get report from test output
        logger.info(f"Grading answer for {instance_id}...")
        report = get_eval_report(
            test_spec=test_spec,
            prediction=pred,
            test_log_path=test_output_path,
            include_tests_status=True,
        )
        logger.info(
            f"report: {report}\n"
            f"Result for {instance_id}: resolved: {report[instance_id]['resolved']}"
        )

        # Write report to report.json
        with open(report_path, "w") as f:
            f.write(json.dumps(report, indent=4))
        
        return instance_id, report

    except EvaluationError as e:
        error_msg = traceback.format_exc()
        logger.info(error_msg)
        print(e)
    except Exception as e:
        error_msg = (
            f"Error in evaluating model for {instance_id}: {e}\n"
            f"{traceback.format_exc()}\n"
            f"Check ({log_file}) for more information."
        )
        logger.error(error_msg)
    finally:
        # Clean up pod
        if pod_name:
            try:
                logger.info(f"Deleting pod {pod_name}")
                v1.delete_namespaced_pod(name=pod_name, namespace=k8s_namespace)
            except Exception as e:
                logger.warning(f"Failed to delete pod {pod_name}: {e}")
        close_logger(logger)
    
    return None


def run_instances_k8s(
    predictions: dict,
    dataset: list,
    full_dataset: list,
    run_id: str,
    timeout: int,
    image_namespace: str = "swebench",
    image_tag: str = "latest",
    k8s_namespace: str = "default",
    max_workers: int = 4,
    rewrite_reports: bool = False,
):
    """
    Run all instances for the given predictions on Kubernetes in parallel.

    Args:
        predictions: Predictions dict generated by the model
        dataset: List of instances to run
        full_dataset: Full dataset for reporting
        run_id: Run ID
        timeout: Timeout for running tests
        k8s_namespace: Kubernetes namespace to use
        max_workers: Maximum number of concurrent pods
        rewrite_reports: True if eval run is just to reformat existing reports
    """
    print(f"Running {len(dataset)} instances on Kubernetes...")
    
    # Create test specs
    from swebench.harness.test_spec.test_spec import make_test_spec
    test_specs = [
        make_test_spec(instance, namespace=image_namespace, instance_image_tag=image_tag) 
        for instance in dataset
    ]
    
    # Prepare payloads for parallel execution
    payloads = []
    for test_spec in test_specs:
        payloads.append((
            test_spec,
            predictions[test_spec.instance_id],
            run_id,
            k8s_namespace,
            timeout,
            rewrite_reports,
        ))
    
    # Run instances in parallel
    run_threadpool(run_instance_k8s, payloads, max_workers)
    print("All Kubernetes instances completed.")
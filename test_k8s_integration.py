#!/usr/bin/env python3
"""
Test script to verify Kubernetes evaluation functionality.
This script tests the basic Kubernetes integration without running full evaluations.
"""

import sys
from pathlib import Path

# Add the swebench module to the path
sys.path.insert(0, str(Path(__file__).parent))

def test_k8s_credentials():
    """Test that Kubernetes credentials can be validated."""
    try:
        from swebench.harness.k8s_eval import validate_k8s_credentials
        validate_k8s_credentials()
        print("✓ Kubernetes credentials validation passed")
        return True
    except Exception as e:
        print(f"✗ Kubernetes credentials validation failed: {e}")
        return False

def test_imports():
    """Test that all required imports work."""
    try:
        import kubernetes
        from kubernetes import client, config
        from swebench.harness.k8s_eval import (
            validate_k8s_credentials,
            run_instances_k8s,
            create_pod_spec,
            wait_for_pod_ready,
            copy_content_to_pod,
            exec_command_in_pod,
            run_instance_k8s,
        )
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        print("Make sure to install the kubernetes library: pip install kubernetes")
        return False

def main():
    """Run basic tests."""
    print("Testing Kubernetes integration for SWE-bench...")
    print()
    
    # Test imports
    if not test_imports():
        return False
    
    # Test credentials (this will fail if no K8s cluster is configured)
    test_k8s_credentials()
    
    print()
    print("Basic tests completed. To use Kubernetes evaluation:")
    print("1. Ensure you have a Kubernetes cluster configured and kubectl working")
    print("2. Run with --kubernetes true --kubernetes_namespace <namespace>")
    print("3. Images must be available on Docker Hub (no local building)")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
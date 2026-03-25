#! /usr/bin/env python3
import argparse
import subprocess
import os
import re
import sys

def find_dependency(directory: str, component: str) -> set[str]:

    directory = directory.rstrip("/")
    component = component.replace("/main.nf", "")

    # Use grep with word boundaries to preempt substring matches.
    grep_results = subprocess.run(
        ["grep", "-r", f"\\b{component}\\b", directory], capture_output=True, text=True
    )

    # Relevant line triggers
    relevant_triggers = ["process", "include", "workflow"]

    # Extract file paths from relevant lines
    file_paths = {
        line.split(":", 1)[0]
        for line in grep_results.stdout.splitlines()
        if ":" in line
    }
    file_paths = set()
    for line in grep_results.stdout.splitlines():
        if any(trigger in line for trigger in relevant_triggers):
                file = line.split(":", 1)[0]
                if "nf-test" in file:
                    continue
                else:
                    file_paths.add(file)

    return file_paths

def identify_component_file(component: str) -> str | None:
    # Search in subworkflows and workflow directories
    search_dirs = ["subworkflows/local/", "workflows/"]

    pattern = f"\\b{component}\\b"
    for d in search_dirs:
        out = subprocess.run(
            ["grep", "-r", pattern, d], capture_output=True, text=True
        ).stdout.splitlines()

        for hit in out:
            path, line_content = hit.split(":", 1)
            if line_content.lstrip().startswith("workflow "):
                # TODO: This could pose a problem when there are multiple matches, and the first match is erroneous.
                return path
    return None

def get_subcomponents(component_path: str) -> tuple[set[str], set[str]]:
    with open(component_path, "r") as f:
        component_content = f.read()

    modules, workflows = set(), set()
    for line in component_content.splitlines():
        if "include" in line:
            # Extract the first all caps variable in the include line
            match = re.search(r'include\s*{\s*([A-Z][A-Z0-9_]*)', line)
            if match:
                if "subworkflows" in line:
                    workflows.add(match.group(1))
                else:
                    modules.add(match.group(1))
                continue
    return modules, workflows

def collect_component_dependencies(component: str) -> tuple[set[str] | None, set[str] | None]:
    file_path = identify_component_file(component)
    if file_path is None:
        return None, None
    modules, workflows = get_subcomponents(file_path)

    for wf in list(workflows):
        sub_mods, sub_wfs = collect_component_dependencies(wf)
        if sub_mods is not None:
            modules.update(sub_mods)
        if sub_wfs is not None:
            workflows.update(sub_wfs)

    return modules, workflows

def workflow_uses_subworkflow(workflow: str, subworkflow_path: str) -> bool:
    workflow_path = f"workflows/{workflow}"
    subworkflow_dir = subworkflow_path.replace("/main.nf", "")

    with open(workflow_path, "r") as f:
        workflow_content = f.read()

    # Use word boundaries to preempt substring matches
    if re.search(r'\b' + re.escape(subworkflow_dir) + r'\b', workflow_content):
        return True
    else:
        return False


def subworkflow_uses_subworkflow(subworkflow: str, dependent_subworkflow: str) -> bool:
    subworkflow_path = f"subworkflows/local/{subworkflow}/main.nf"
    dependent_subworkflow_dir = dependent_subworkflow.replace("/main.nf", "")

    with open(subworkflow_path, "r") as f:
        subworkflow_content = f.read()

    # Use word boundaries to preempt substring matches
    if re.search(r'\b' + re.escape(dependent_subworkflow_dir) + r'\b', subworkflow_content):
        return True
    else:
        return False


def get_workflow_test(workflow: str) -> str:
    workflow_path = f"workflows/{workflow}"
    if re.search(r'\b' + re.escape("run_dev_se") + r'\b', workflow_path):
        return "tests/workflows/run_dev.nf.test"
    else:
        base_name = os.path.basename(workflow_path).split(".")[0]
        return f"tests/workflows/{base_name}.nf.test"


def parse_args() -> tuple[str, bool, bool]:
    """Parse arguments and run the test finder."""
    parser = argparse.ArgumentParser(
        description="Find and run tests for specified components"
    )
    parser.add_argument(
        "-c",
        "--component",
        required=True,
        help="Name of component (process or subworkflow) to test",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose nf-test output",
        default=False,
    )

    parser.add_argument(
        "-s",
        "--skip-subcomponents",
        action="store_true",
        help="Skip testing subcomponents of the component. Only applicable if the component is a workflow/subworkflow.",
        default=True,
    )

    args = parser.parse_args()
    component = args.component
    verbose = args.verbose
    skip_subcomponents = args.skip_subcomponents
    return component, verbose, skip_subcomponents


def main() -> None:
    component, verbose, skip_subcomponents = parse_args()

    # Validate component name - only allow alphanumerics and underscores
    if not re.match(r'^[a-zA-Z0-9_]+$', component):
        raise ValueError(f"Error: Invalid component name '{component}'. Component names must contain only alphanumeric characters and underscores.")

    tests_to_execute = set()


    #-----------------------------------------------------------------#
    # Identifying tests that directly use the component
    #-----------------------------------------------------------------#

    direct_tests = find_dependency("tests/", component)
    tests_to_execute.update(direct_tests)

    print("=" * 72)
    if len(direct_tests) == 0:
        print("No direct tests found.")
    elif len(direct_tests) == 1:
        print("Found 1 direct test:")
    else:
        print(f"Found {len(direct_tests)} direct tests:")
    print("=" * 72)
    for test in direct_tests:
        print(f"   • {test}")
    print()

    #-----------------------------------------------------------------#
    # Identifying dependents
    #-----------------------------------------------------------------#

    # Find dependent subworkflows
    dependent_subworkflows = set(find_dependency("subworkflows/", component))

    # Find transitive dependencies - subworkflows that use other dependent subworkflows
    transitive_dependencies = set()
    for subworkflow in os.listdir("subworkflows/local/"):
        for dependent_subworkflow in dependent_subworkflows:
            if subworkflow_uses_subworkflow(subworkflow, dependent_subworkflow):
                transitive_dependencies.add(subworkflow)

    # Combine all dependent subworkflows
    dependent_subworkflows.update(transitive_dependencies)

    # Identifying tests for dependent subworkflows
    subworkflow_tests = set()
    for dependent_subworkflow in dependent_subworkflows:
        tests = find_dependency("tests/subworkflows/", dependent_subworkflow)
        subworkflow_tests.update(tests)

    print("=" * 72)
    print(f"Found {len(subworkflow_tests)} tests for affected subworkflows:")
    print("=" * 72)
    for test in subworkflow_tests:
        print(f"   • {test}")
    print()

    # Identifying dependent workflows
    dependent_workflows = set()
    dependent_workflows.update(find_dependency("workflows/", component))

    # Identifying dependent workflows of dependent subworkflows
    for workflow in os.listdir("workflows/"):
        for dependent_subworkflow in dependent_subworkflows:
            if workflow_uses_subworkflow(workflow, dependent_subworkflow):
                dependent_workflows.add(workflow)

    # Identifying tests for dependent workflows
    workflow_tests = set()
    for workflow in dependent_workflows:
        workflow_tests.add(get_workflow_test(workflow))


    print("=" * 72)
    print(f"Found {len(workflow_tests)} tests for affected workflows:")
    print("=" * 72)
    for test in workflow_tests:
        print(f"   • {test}")
    print()

    #-----------------------------------------------------------------#
    # Identifying dependencies
    #-----------------------------------------------------------------#

    # Identifying module and subworkflow dependencies and their tests
    if not skip_subcomponents:
        subcomp_tests = set()
        modules, workflows = collect_component_dependencies(component)
        if modules is None and workflows is None:
            print("=" * 72)
            print(f"No subcomponents found for {component}.")

        else:
            if modules is not None:
                for module in modules:
                    subcomp_tests.update(find_dependency("tests/modules/local/", module))
            if workflows is not None:
                for workflow in workflows:
                    subcomp_tests.update(find_dependency("tests/subworkflows/local/", workflow))


            print("=" * 72)
            print(f"Found {len(subcomp_tests)} tests for subcomponents of {component}:")
            print("=" * 72)

            for test in sorted(subcomp_tests):
                print(f"   • {test}")
            print()
            tests_to_execute.update(subcomp_tests)


    tests_to_execute.update(subworkflow_tests)
    tests_to_execute.update(workflow_tests)

    #-----------------------------------------------------------------#
    # Running tests
    #-----------------------------------------------------------------#

    print("=" * 72)
    print(f"Identified {len(tests_to_execute)} test files to execute. Running...")

    # Run all tests in a single nf-test command
    cmd = ["nf-test", "test"] + sorted(tests_to_execute)
    if verbose:
        cmd.append("--verbose")
        print(f"\nRunning test files with verbose output:")
    else:
        print(f"\nRunning test files:")

    for test in sorted(tests_to_execute):
        print(f"   • {test}")
    print("-" * 72)

    result = subprocess.run(cmd)


if __name__ == "__main__":
    main()

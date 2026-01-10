#!/usr/bin/env python

#=============================================================================
# Imports
#=============================================================================

import os
import re
import sys
import argparse
from pathlib import Path
from typing import Dict, Set, List
from dataclasses import dataclass, field

#=============================================================================
# Dataclasses
#=============================================================================

@dataclass
class Process:
    name: str
    file_path: Path
    line_number: int
    module_name: str = None
    testing_only: bool = False

@dataclass
class Module:
    name: str
    path: Path
    processes: Dict[str, Process] = field(default_factory = dict)

@dataclass
class Workflow:
    name: str
    file_path: Path
    is_anonymous: bool = False

#=============================================================================
# Main class
#=============================================================================

class NextflowAnalyzer:

    def __init__(self, pipeline_dir: str):
        # Directories
        self.pipeline_dir = Path(pipeline_dir)
        self.workflow_dir = self.pipeline_dir / "workflows"
        self.subworkflow_dir = self.pipeline_dir / "subworkflows" / "local"
        self.module_dir = self.pipeline_dir / "modules" / "local"
        # Initialize dictionaries
        self.workflows: Dict[str, Workflow] = {}
        self.modules: Dict[str, Module] = {}
        self.standalone_processes: Dict[str, Process] = {}
        self.workflow_dependencies: Dict[str, Dict[str, Set[str]]] = {}
        self.unused_components: Dict[str, Set[str]] = {}
        # Scan contents and construct lists
        self._scan_files()
        self._analyze_dependencies()
        self._find_unused_components()

    def _scan_files(self):
        """Scan directory for Nextflow files and identify workflows, modules and processes."""
        # First check for main.nf in root directory
        main_nf = self.pipeline_dir / "main.nf"
        if main_nf.exists():
            self.workflows["main"] = Workflow(name="main", file_path=main_nf, is_anonymous=True)
        # Scan workflows directory
        if self.workflow_dir.exists():
            for workflow_file in self.workflow_dir.rglob("*.nf"):
                with open(workflow_file) as f:
                    content = f.read()
                if re.search(r'workflow\s*{', content): # No anonymous workflows except main
                    raise ValueError(f"Unexpected anonymous workflow in file: {workflow_file}")
                workflow_matches = re.finditer(r'workflow\s+(\w+)\s*{', content)
                for match in workflow_matches:
                    workflow_name = match.group(1)
                    self.workflows[workflow_name] = Workflow(name=workflow_name, file_path = workflow_file)
        # Scan subworkflows directory
        if self.subworkflow_dir.exists():
            for subworkflow_path in self.subworkflow_dir.glob("**/main.nf"):
                subworkflow_name = subworkflow_path.parent.name
                self.workflows[subworkflow_name] = Workflow(name=subworkflow_name, file_path = subworkflow_path)
        # Scan modules directory
        if self.module_dir.exists():
            for module_path in self.module_dir.glob("**/main.nf"):
                module_name = module_path.parent.name
                self.modules[module_name] = Module(name=module_name, path=module_path)
                self._scan_file_for_processes(module_path, module_name)
        # Scan for standalone processes (excluding modules and workflows directories)
        for file_path in self.pipeline_dir.rglob("*.nf"):
            if ("modules/local" not in str(file_path) and
                "workflows" not in str(file_path) and
                file_path != main_nf):
                self._scan_file_for_processes(file_path)

    def _scan_file_for_processes(self, file_path: Path, module_name: str = None):
        """Scan a file for process definitions."""
        with open(file_path) as f:
            content = f.read()
        # Check if file contains testing_only label
        has_testing_only = bool(re.search(r'label\s+"testing_only"', content))

        for line_num, line in enumerate(content.splitlines(), 1):
            process_match = re.search(r'process\s+(\w+)\s*{', line)
            if process_match:
                process_name = process_match.group(1)
                process = Process(name=process_name, file_path=file_path,
                                  line_number=line_num, module_name=module_name,
                                  testing_only=has_testing_only)
                if module_name:
                    self.modules[module_name].processes[process_name] = process
                else:
                    self.standalone_processes[process_name] = process

    def _analyze_dependencies(self):
        """Extract dependencies from workflows in pipeline."""
        for workflow_name in self.workflows.keys():
            workflow = self.workflows[workflow_name]
            self.workflow_dependencies[workflow.name] = {
                    "modules": set(),
                    "processes": set(),
                    "workflows": set()
                    }
            with open(workflow.file_path) as f:
                content = f.read()
                depend_matches = re.finditer(r'\s*include\s+{\s*(\S*).*}\s+from\s+[\'"](\S*)[\'"]\s*', content)
                for match in depend_matches:
                    item = match.group(1)
                    full_path = match.group(2).rstrip('/')
                    # Split into dirpath and parent
                    parts = full_path.rsplit('/', 1)
                    if len(parts) == 2:
                        dirpath, parent = parts
                    else:
                        dirpath = ''
                        parent = parts[0]
                    if re.search("/workflows", dirpath):
                        self.workflow_dependencies[workflow_name]["workflows"].add(item)
                    elif re.search("/subworkflows/local", dirpath):
                        self.workflow_dependencies[workflow_name]["workflows"].add(parent)
                    elif re.search("/modules/local", dirpath):
                        self.workflow_dependencies[workflow_name]["modules"].add(parent)
                        self.workflow_dependencies[workflow_name]["processes"].add(item)
                    else:
                        self.workflow_dependencies[workflow_name]["processes"].add(item)

    def _find_unused_components(self):
        """Identify modules, processes and workflows that aren't called within the main workflow."""
        used_modules, used_processes, used_workflows = self._extract_dependencies("main", set(), set(), set())
        unused_modules   = {name for name in self.modules.keys() if name not in used_modules}
        unused_workflows = {name for name in self.workflows.keys() if name not in used_workflows}
        unused_processes = {name for name in self.standalone_processes.keys() if name not in used_processes}
        for module_name in self.modules.keys():
            module = self.modules[module_name]
            unused_processes.update([name for name in module.processes.keys() if name not in used_processes])
        self.unused_components["modules"] = unused_modules
        self.unused_components["workflows"] = unused_workflows
        self.unused_components["processes"] = unused_processes

    def _extract_dependencies(self, workflow_name, modules, processes, workflows):
        workflows.add(workflow_name)
        processes.update(self.workflow_dependencies[workflow_name]["processes"])
        modules.update(self.workflow_dependencies[workflow_name]["modules"])
        dependent_workflows = self.workflow_dependencies[workflow_name]["workflows"]
        for dependent_workflow in dependent_workflows:
            modules, processes, workflows = self._extract_dependencies(dependent_workflow, modules, processes, workflows)
        return modules, processes, workflows

#=============================================================================
# Report generation function
#=============================================================================


def generate_nextflow_report(analyzer: NextflowAnalyzer, output_file: str):
    """Generate a detailed human-readable report from an Analyzer object."""
    with open(output_file, "w") as f:
        # Header
        f.write("=========================================\n")
        f.write("=== NEXTFLOW PIPELINE ANALYSIS REPORT ===\n")
        f.write("=========================================\n\n")
        f.write(f"Pipeline directory: {analyzer.pipeline_dir.absolute()}\n\n")
        # Summary statistics
        f.write("==========================\n")
        f.write("=== Summary Statistics ===\n")
        f.write("==========================\n\n")
        f.write(f"Total Workflows: {len(analyzer.workflows)}\n")
        f.write(f"Total Modules: {len(analyzer.modules)}\n")
        total_processes = len(analyzer.standalone_processes) + sum(
            len(m.processes) for m in analyzer.modules.values()
        )
        f.write(f"Total Processes: {total_processes}\n\n")
        f.write(f"Unused Workflows: {len(analyzer.unused_components['workflows'])}\n")
        f.write(f"Unused Modules: {len(analyzer.unused_components['modules'])}\n")
        f.write(f"Unused Processes: {len(analyzer.unused_components['processes'])}\n\n")
        # Workflow summary
        report_workflows(analyzer, f)
        # Module summary
        report_modules(analyzer, f)
        # Standalone processes summary
        report_standalone_processes(analyzer, f)
        # Unused components summary
        report_unused_components(analyzer, f)

def report_workflows(analyzer: NextflowAnalyzer, output_stream):
    """Write workflow section of Nextflow report."""
    output_stream.write("=================\n")
    output_stream.write("=== Workflows ===\n")
    output_stream.write("=================\n\n")
    # Order workflows to start with main
    workflow_order = []
    if "main" in analyzer.workflows:
        workflow_order.append("main")
    workflow_order.extend(sorted(name for name in analyzer.workflows if name not in workflow_order))
    for name in workflow_order:
        # Print basic workflow information
        workflow = analyzer.workflows[name]
        output_stream.write(f"Workflow: {name}\n")
        output_stream.write(f"Location: {workflow.file_path.relative_to(analyzer.pipeline_dir)}\n")
        # Print information about dependencies
        if name in analyzer.workflow_dependencies:
            deps = analyzer.workflow_dependencies[name]
            dep_workflows = sorted(deps["workflows"])
            if dep_workflows:
                output_stream.write("Calls (sub)workflows:\n")
                for workflow in dep_workflows:
                    output_stream.write(f"\t- {workflow}\n")
            else:
                output_stream.write("Calls no (sub)workflows.\n")
            dep_modules = sorted(deps["modules"])
            if dep_modules:
                output_stream.write("Uses modules:\n")
                for module in dep_modules:
                    output_stream.write(f"\t- {module}\n")
            else:
                output_stream.write("Uses no modules.\n")
            dep_processes = sorted(deps["processes"])
            if dep_processes:
                output_stream.write("Uses processes:\n")
                for process in dep_processes:
                    output_stream.write(f"\t- {process}\n")
            else:
                output_stream.write("Uses no processes.\n")
        # Collate workflows that call this one
        called_by = [
                workflow for workflow in analyzer.workflow_dependencies.keys()
                if name in analyzer.workflow_dependencies[workflow]["workflows"]
                ]
        if called_by:
            output_stream.write("Called by workflows:\n")
            for workflow in sorted(called_by):
                output_stream.write(f"\t- {workflow}\n")
        elif name not in ["main"]:
            output_stream.write("Not called by any workflow.\n")
        output_stream.write("\n")

def report_modules(analyzer: NextflowAnalyzer, output_stream):
    """Write module section of Nextflow report."""
    output_stream.write("===============\n")
    output_stream.write("=== Modules ===\n")
    output_stream.write("===============\n\n")
    if not analyzer.modules:
        output_stream.write("No modules in pipeline.\n\n")
    for name in sorted(analyzer.modules.keys()):
        module = analyzer.modules[name]
        # Basic module information
        output_stream.write(f"Module: {name}\n")
        output_stream.write(f"Location: {module.path.relative_to(analyzer.pipeline_dir)}\n")
        # Processes in module
        if module.processes:
            output_stream.write("Contains processes:\n")
            for proc_name, process in sorted(module.processes.items()):
                usage_status = " (UNUSED)" if proc_name in analyzer.unused_components['processes'] else ""
                output_stream.write(f"\t- {proc_name}{usage_status}\n")
        else:
            output_stream.write("Contains no processes.\n")
        # Parent workflows
        called_by = [
            workflow for workflow in analyzer.workflow_dependencies.keys()
            if name in analyzer.workflow_dependencies[workflow]["modules"]
            ]
        if called_by:
            output_stream.write("Used by workflows:\n")
            for workflow in sorted(called_by):
                output_stream.write(f"\t- {workflow}\n")
        else:
            output_stream.write("Not used by any workflow.\n")
        output_stream.write("\n")

def report_standalone_processes(analyzer: NextflowAnalyzer, output_stream):
    """Write standalone processes section of Nextflow report."""
    output_stream.write("============================\n")
    output_stream.write("=== Standalone Processes ===\n")
    output_stream.write("============================\n\n")
    if not analyzer.standalone_processes:
        output_stream.write("No standalone processes in pipeline.\n\n")
    for name in sorted(analyzer.standalone_processes.keys()):
        process = analyzer.standalone_processes[name]
        # Basic process information
        output_stream.write(f"Process: {name}\n")
        output_stream.write(f"Location: {process.file_path.relative_to(analyzer.pipeline_dir)}\n")
        # Parent workflows
        called_by = [
            workflow for workflow in analyzer.workflow_dependencies.keys()
            if name in analyzer.workflow_dependencies[workflow]["processes"]
            ]
        if called_by:
            output_stream.write("Used by workflows:\n")
            for workflow in sorted(called_by):
                output_stream.write(f"\t- {workflow}\n")
        else:
            output_stream.write("Not used by any workflow.\n")
        output_stream.write("\n")

def report_unused_components(analyzer: NextflowAnalyzer, output_stream):
    """Write unused components section of Nextflow report."""
    output_stream.write("=========================\n")
    output_stream.write("=== Unused Components ===\n")
    output_stream.write("=========================\n\n")
    unused = analyzer.unused_components

    # Unused workflows
    if unused["workflows"]:
        output_stream.write("Unused workflows:\n")
        for name in sorted(unused["workflows"]):
            path = analyzer.workflows[name].file_path.relative_to(analyzer.pipeline_dir)
            output_stream.write(f"\t- {name} ({path})\n")
        output_stream.write("\n")
    else:
        output_stream.write("No unused workflows found.\n\n")

    # Separate unused modules into testing_only and regular
    unused_modules_testing = []
    unused_modules_regular = []
    for name in unused["modules"]:
        module = analyzer.modules[name]
        # Check if any process in the module has testing_only
        if any(proc.testing_only for proc in module.processes.values()):
            unused_modules_testing.append(name)
        else:
            unused_modules_regular.append(name)

    # Report regular unused modules
    if unused_modules_regular:
        output_stream.write("Unused modules:\n")
        for name in sorted(unused_modules_regular):
            path = analyzer.modules[name].path.relative_to(analyzer.pipeline_dir)
            output_stream.write(f"\t- {name} ({path})\n")
        output_stream.write("\n")
    else:
        output_stream.write("No unused modules found.\n\n")

    # Report testing_only unused modules
    if unused_modules_testing:
        output_stream.write("Unused modules (testing only):\n")
        for name in sorted(unused_modules_testing):
            path = analyzer.modules[name].path.relative_to(analyzer.pipeline_dir)
            output_stream.write(f"\t- {name} ({path})\n")
        output_stream.write("\n")

    # Separate unused processes into testing_only and regular
    unused_processes_testing = []
    unused_processes_regular = []
    for name in unused["processes"]:
        if name in analyzer.standalone_processes:
            process = analyzer.standalone_processes[name]
        else:
            # Find the process in modules
            parent_module = [module for module in analyzer.modules.keys()
                             if name in analyzer.modules[module].processes][0]
            process = analyzer.modules[parent_module].processes[name]

        if process.testing_only:
            unused_processes_testing.append(name)
        else:
            unused_processes_regular.append(name)

    # Report regular unused processes
    if unused_processes_regular:
        output_stream.write("Unused processes:\n")
        for name in sorted(unused_processes_regular):
            if name in analyzer.standalone_processes.keys():
                path = analyzer.standalone_processes[name].file_path.relative_to(analyzer.pipeline_dir)
            else:
                parent_module = [module for module in analyzer.modules.keys()
                                 if name in analyzer.modules[module].processes][0]
                path = analyzer.modules[parent_module].path.relative_to(analyzer.pipeline_dir)
            output_stream.write(f"\t- {name} ({path})\n")
        output_stream.write("\n")
    else:
        output_stream.write("No unused processes found.\n\n")

    # Report testing_only unused processes
    if unused_processes_testing:
        output_stream.write("Unused processes (testing only):\n")
        for name in sorted(unused_processes_testing):
            if name in analyzer.standalone_processes.keys():
                path = analyzer.standalone_processes[name].file_path.relative_to(analyzer.pipeline_dir)
            else:
                parent_module = [module for module in analyzer.modules.keys()
                                 if name in analyzer.modules[module].processes][0]
                path = analyzer.modules[parent_module].path.relative_to(analyzer.pipeline_dir)
            output_stream.write(f"\t- {name} ({path})\n")
        output_stream.write("\n")

#=============================================================================
# Run script
#=============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Analyze Nextflow pipeline to find unused workflows, modules, and processes.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-d", "--pipeline_dir", default=os.getcwd(),
                        help="Path to the Nextflow pipeline directory (default: current working directory)")
    parser.add_argument("-o", "--output_path", default="pipeline_report.txt",
                        help="Path to output file for detailed report (default: pipeline_report.txt)")
    return parser.parse_args()

def main():
    # Parse arguments
    args = parse_args()
    pipeline_dir = args.pipeline_dir
    output_path = args.output_path
    # Check pipeline directory exists
    if not os.path.isdir(pipeline_dir):
        print(f"Error: Directory '{pipeline_dir}' does not exist", file=sys.stderr)
        sys.exit(1)
    # Initialize analyzer object
    analyzer = NextflowAnalyzer(pipeline_dir)
    # Create report
    generate_nextflow_report(analyzer, output_path)

if __name__ == "__main__":
    main()

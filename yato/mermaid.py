from yato.parser import Dependency


def generate_mermaid_diagram(folder: str, dependencies: Dependency) -> None:
    mermaid_diagram = "flowchart LR\n"
    for node_name, node in dependencies.items():
        mermaid_diagram += f"  {node_name}({node_name})\n"

        for dep in node.deps:
            mermaid_diagram += f"  {dep} --> {node_name}\n"

    with open(f"{folder}/lineage.mmd", "w") as f:
        f.write(mermaid_diagram)

    return mermaid_diagram

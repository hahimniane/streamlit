"""
Industry (NAICS) Filtering and Selection
- NAICS industry codes reference data
- Hierarchical NAICS industry selector for filtering facilities
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import streamlit as st
from core.data_loader import load_naics_dict

# Import st_ant_tree for dropdown tree selector
try:
    from st_ant_tree import st_ant_tree
    ANT_TREE_AVAILABLE = True
except ImportError:
    ANT_TREE_AVAILABLE = False


# =============================================================================
# NAICS Industry Codes - Loaded from data/naics_2022.csv via core.data_loader
# =============================================================================
# Callers should use core.data_loader.load_naics_dict() and pass the result
# as naics_dict to render_hierarchical_naics_selector().


def build_naics_hierarchy(naics_dict: Dict[str, str]) -> Dict[str, Dict]:
    """
    Build a hierarchical structure from flat NAICS dictionary.
    Nests codes under their longest existing ancestor in the dictionary.
    """
    hierarchy: Dict[str, Dict] = {}
    nodes: Dict[str, Dict] = {}

    # Create a node for every code in the dictionary
    for code, name in sorted(naics_dict.items()):
        nodes[code] = {
            "name": name,
            "children": {},
            "code": code,
        }

    # Virtual parent for Manufacturing (31, 32, 33) so it appears as one expandable root
    manufacturing_children = {c: nodes[c] for c in ("31", "32", "33") if c in nodes}
    if len(manufacturing_children) >= 2:
        nodes["31-33"] = {
            "name": "Manufacturing (31–33)",
            "children": manufacturing_children,
            "code": "31-33",
            "_virtual": True,
        }

    # Organize into hierarchy: parent = longest existing prefix of code
    for code, node in sorted(nodes.items()):
        if node.get("_virtual"):
            continue
        parent_code = None
        for i in range(len(code) - 1, 1, -1):
            prefix = code[:i]
            if prefix in nodes:
                parent_code = prefix
                break
        if parent_code:
            nodes[parent_code]["children"][code] = node
        else:
            hierarchy[code] = node

    # Single Manufacturing root (31-33) instead of three roots 31, 32, 33
    if "31-33" in nodes:
        for c in ("31", "32", "33"):
            hierarchy.pop(c, None)
        hierarchy["31-33"] = nodes["31-33"]

    return hierarchy


def convert_to_ant_tree_format(hierarchy: Dict[str, Dict]) -> List[Dict]:
    """
    Convert hierarchy to st_ant_tree format.
    
    Format: [{"value": "code", "title": "Name (Code)", "children": [...]}]
    """
    tree_data = []

    def process_node(code: str, data: Dict) -> Dict:
        is_virtual = bool(data.get("_virtual")) or ("-" in code)
        title = data["name"] if is_virtual else f"{data['name']} ({code})"
        
        node = {
            "value": code,
            "title": title,
        }
        if is_virtual:
            node["selectable"] = False
        children = data.get("children", {})
        if children:
            node["children"] = [
                process_node(child_code, child_data)
                for child_code, child_data in sorted(children.items())
            ]
        return node

    for code, data in sorted(hierarchy.items()):
        tree_data.append(process_node(code, data))

    return tree_data


def render_hierarchical_naics_selector(
    naics_dict: Dict[str, str],
    key: str,
    default_index: int = 0,
    default_value: Optional[str] = None,
    use_sidebar: bool = True,
    multi_select: bool = False,
    allow_empty: bool = False,
) -> List[str] | str:
    """
    Render a hierarchical NAICS industry selector using st_ant_tree dropdown.
    """
    hierarchy = build_naics_hierarchy(naics_dict)

    if ANT_TREE_AVAILABLE:
        tree_data = convert_to_ant_tree_format(hierarchy)
        default_val = [default_value] if default_value else None

        with st.sidebar if use_sidebar else st.container():
            kwargs = dict(
                treeData=tree_data,
                placeholder="Select Industry Type...",
                allowClear=True,
                showSearch=True,
                treeLine=True,
                defaultValue=default_val,
                key=key,
                multiple=False,
                treeCheckable=False,
            )
            if multi_select:
                kwargs.update(
                    dict(
                        multiple=True,
                        treeCheckable=True,
                        treeCheckStrictly=True,
                    )
                )

            try:
                selected = st_ant_tree(**kwargs)
            except TypeError:
                problematic = ["treeCheckStrictly", "treeCheckable", "multiple"]
                for bad in problematic:
                    kwargs.pop(bad, None)
                try:
                    selected = st_ant_tree(**kwargs)
                except TypeError:
                    selected = st_ant_tree(
                        treeData=tree_data,
                        placeholder="Select Industry Type...",
                        allowClear=True,
                        key=key,
                    )

        if selected:
            if isinstance(selected, list):
                if len(selected) > 0:
                    return selected if multi_select else selected[0]
            elif isinstance(selected, str):
                return [selected] if multi_select else selected
            elif isinstance(selected, (int, float)):
                code = str(int(selected))
                return [code] if multi_select else code
        
        if default_value:
            return [default_value] if multi_select else default_value
        elif allow_empty:
            return [] if multi_select else ""
        else:
            fallback = list(naics_dict.keys())[default_index] if naics_dict else ""
            return [fallback] if multi_select and fallback else fallback

    else:
        container = st.sidebar if use_sidebar else st
        return _render_fallback_selector(
            hierarchy,
            naics_dict,
            key,
            default_index,
            container,
            multi_select=multi_select,
            allow_empty=allow_empty,
        )


def format_naics_display(
    selected_naics_code: Optional[str],
    naics_dict: Dict[str, str],
    empty_label: str = "All Industries",
) -> str:
    """Format selected NAICS code for parameter display."""
    if not selected_naics_code:
        return empty_label
    return f"{selected_naics_code} - {naics_dict.get(selected_naics_code, 'Unknown')}"


def render_sidebar_industry_selector(
    analysis_key: str,
    heading: str = "### Industry Type",
    caption: Optional[str] = None,
    allow_empty: bool = True,
    empty_label: str = "All Industries",
) -> Tuple[str, str]:
    """
    Render a standardized sidebar NAICS selector and return selected code + display label.

    Returns:
        (selected_naics_code, selected_industry_display)
    """
    naics_dict = load_naics_dict()
    st.sidebar.markdown(heading)
    if caption:
        st.sidebar.markdown(caption)

    selected_naics_code = render_hierarchical_naics_selector(
        naics_dict=naics_dict,
        key=f"{analysis_key}_industry_selector",
        default_value=None,
        allow_empty=allow_empty,
    )
    if isinstance(selected_naics_code, list):
        selected_naics_code = selected_naics_code[0] if selected_naics_code else ""
    selected_industry_display = format_naics_display(
        selected_naics_code=selected_naics_code,
        naics_dict=naics_dict,
        empty_label=empty_label,
    )
    return selected_naics_code, selected_industry_display


def _render_fallback_selector(
    hierarchy: Dict[str, Dict],
    naics_dict: Dict[str, str],
    key: str,
    default_index: int,
    container=None,
    multi_select: bool = False,
    allow_empty: bool = False,
) -> List[str] | str:
    """Fallback selector using indented selectbox."""
    if container is None:
        container = st

    options = []
    code_to_option = {}

    def add_to_options(node_code: str, node_data: Dict, level: int = 0):
        name = node_data["name"]
        indent = "  " * level
        prefix = "├─ " if level > 0 else ""
        display_name = f"{indent}{prefix}{node_code} - {name}"
        options.append(display_name)
        code_to_option[node_code] = display_name

        for child_code, child_data in sorted(node_data.get("children", {}).items()):
            add_to_options(child_code, child_data, level + 1)

    for code, data in sorted(hierarchy.items()):
        add_to_options(code, data, level=0)

    option_to_code = {v: k for k, v in code_to_option.items()}

    if multi_select:
        default_option = options[default_index] if options and not allow_empty else None
        selected_options = container.multiselect(
            "Select Industry Type",
            options=options,
            default=[default_option] if default_option else [],
            key=key,
            help="Select NAICS industry codes"
        )
        return [
            option_to_code.get(option, "")
            for option in selected_options
            if option in option_to_code
        ]

    if allow_empty:
        options = ["-- All Industries --"] + options

    selected_display = container.selectbox(
        "Select Industry Type",
        options=options,
        index=0 if allow_empty else default_index,
        key=key,
        help="Select NAICS industry code"
    )

    if allow_empty and selected_display == "-- All Industries --":
        return ""
    return option_to_code.get(selected_display, list(naics_dict.keys())[0] if naics_dict else "")

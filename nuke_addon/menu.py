"""Nuke menu integration for NukeMCP.

Place this file, nuke_mcp_addon.py and NukeMCP_icon.png in your .nuke directory
or NUKE_PATH. Nuke finds the icon by filename on the plugin path.
"""

import nuke

toolbar = nuke.menu("Nodes")
nuke_mcp_menu = toolbar.addMenu("NukeMCP", icon="NukeMCP_icon.png")
nuke_mcp_menu.addCommand(
    "Start Server",
    "import nuke_mcp_addon; nuke_mcp_addon.start()",
)

"""Nuke menu integration for NukeMCP.

Place this file, nuke_mcp_addon.py and NukeMCP_icon.png in your .nuke directory
or NUKE_PATH. Nuke finds the icon by filename on the plugin path.
"""

import nuke

toolbar = nuke.menu("Nodes")
# # Use this if more menu items are ever added:
# nuke_mcp_menu = toolbar.addMenu("NukeMCP", icon="NukeMCP_icon.png")
# nuke_mcp_menu.addCommand(
#     "Start Server",
#     "import nuke_mcp_addon; nuke_mcp_addon.start()",
# )

# # One click to start while only one action exists:
toolbar.addCommand("StartNukeMCP", "import nuke_mcp_addon; nuke_mcp_addon.start()", icon="NukeMCP_icon.png")

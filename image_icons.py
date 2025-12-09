"""
Image and icon mapping for presentation slides.
Maps image descriptions to emoji, Font Awesome icons, or image paths.
"""

# Emoji mappings for common icons
EMOJI_ICONS = {
    "bronze": "🥉",
    "silver": "🥈",
    "gold": "🥇",
    "medal": "🏅",
    "checkmark": "✓",
    "warning": "⚠️",
    "alert": "⚠️",
    "clock": "⏰",
    "time": "⏰",
    "dollar": "💲",
    "cost": "💲",
    "money": "💰",
    "roadmap": "🗺️",
    "checklist": "✅",
    "list": "📋",
    "code": "💻",
    "database": "🗄️",
    "table": "📊",
    "dashboard": "📈",
    "chart": "📊",
    "graph": "📈",
    "analytics": "📊",
    "arrow": "➡️",
    "flow": "➡️",
    "pipeline": "🔧",
    "gear": "⚙️",
    "tools": "🔧",
    "shield": "🛡️",
    "security": "🛡️",
    "lock": "🔒",
    "cloud": "☁️",
    "databricks": "☁️",
    "spark": "⚡",
    "speed": "⚡",
    "performance": "⚡",
    "question": "❓",
    "qa": "❓",
    "learning": "📚",
    "education": "📚",
    "book": "📖",
    "documentation": "📄",
    "logo": "🏢",
    "company": "🏢",
}

# Font Awesome icon mappings (using CDN)
FONT_AWESOME_ICONS = {
    "databricks": "fa-cloud",
    "pipeline": "fa-project-diagram",
    "database": "fa-database",
    "table": "fa-table",
    "dashboard": "fa-chart-line",
    "analytics": "fa-chart-bar",
    "code": "fa-code",
    "gear": "fa-cog",
    "tools": "fa-tools",
    "shield": "fa-shield-alt",
    "security": "fa-lock",
    "clock": "fa-clock",
    "time": "fa-clock",
    "dollar": "fa-dollar-sign",
    "money": "fa-money-bill",
    "checkmark": "fa-check-circle",
    "warning": "fa-exclamation-triangle",
    "alert": "fa-exclamation-circle",
    "arrow": "fa-arrow-right",
    "flow": "fa-arrow-right",
    "roadmap": "fa-route",
    "checklist": "fa-tasks",
    "list": "fa-list",
    "chart": "fa-chart-pie",
    "graph": "fa-chart-line",
    "question": "fa-question-circle",
    "qa": "fa-question-circle",
    "learning": "fa-graduation-cap",
    "education": "fa-book",
    "logo": "fa-building",
    "company": "fa-building",
}


# SVG icon generators for common shapes
def get_svg_icon(icon_type: str, color: str = "#0066CC", size: int = 48) -> str:
    """Generate SVG icon code."""
    icons = {
        "medal-bronze": f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="{color}"><circle cx="12" cy="12" r="10"/><text x="12" y="16" font-size="12" fill="white" text-anchor="middle">B</text></svg>',
        "medal-silver": f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="{color}"><circle cx="12" cy="12" r="10"/><text x="12" y="16" font-size="12" fill="white" text-anchor="middle">S</text></svg>',
        "medal-gold": f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="{color}"><circle cx="12" cy="12" r="10"/><text x="12" y="16" font-size="12" fill="white" text-anchor="middle">G</text></svg>',
        "arrow-right": f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="{color}"><path d="M8.59 16.59L13.17 12 8.59 7.41 10 6l6 6-6 6-1.41-1.41z"/></svg>',
        "check": f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="{color}"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>',
    }
    return icons.get(icon_type, "")


def get_icon_for_description(description: str) -> str:
    """Get appropriate icon (emoji or Font Awesome) for image description."""
    desc_lower = description.lower()

    # Check for specific matches
    for key, emoji in EMOJI_ICONS.items():
        if key in desc_lower:
            return emoji

    # Default fallback
    return "📌"


def get_font_awesome_class(description: str) -> str:
    """Get Font Awesome class for description."""
    desc_lower = description.lower()

    for key, fa_class in FONT_AWESOME_ICONS.items():
        if key in desc_lower:
            return fa_class

    return "fa-image"  # Default

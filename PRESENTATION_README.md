# Presentation Markdown Guide

This directory contains a clean, well-structured markdown presentation that can be used with various presentation tools.

## File: `PRESENTATION.md`

A professional presentation about Pipeline Builder on Databricks, formatted for easy conversion to slides.

## How to Use

### Option 1: Marp (Recommended)

Marp is a markdown presentation ecosystem. It's the easiest way to present markdown slides.

**Install:**
```bash
npm install -g @marp-team/marp-cli
```

**Convert to HTML:**
```bash
marp PRESENTATION.md -o presentation.html
```

**Convert to PDF:**
```bash
marp PRESENTATION.md -o presentation.pdf
```

**Convert to PowerPoint:**
```bash
marp PRESENTATION.md -o presentation.pptx
```

**Live Preview:**
```bash
marp PRESENTATION.md --preview
```

### Option 2: reveal-md

Convert to Reveal.js presentation.

**Install:**
```bash
npm install -g reveal-md
```

**Present:**
```bash
reveal-md PRESENTATION.md
```

**Export to HTML:**
```bash
reveal-md PRESENTATION.md --static presentation.html
```

### Option 3: Pandoc

Convert to PowerPoint, PDF, or other formats.

**Install:**
```bash
# macOS
brew install pandoc

# Or download from: https://pandoc.org/installing.html
```

**Convert to PowerPoint:**
```bash
pandoc PRESENTATION.md -o presentation.pptx
```

**Convert to PDF:**
```bash
pandoc PRESENTATION.md -o presentation.pdf
```

**Convert to HTML:**
```bash
pandoc PRESENTATION.md -o presentation.html --standalone --css style.css
```

### Option 4: VS Code Extension

**Marp for VS Code:**
1. Install "Marp for VS Code" extension
2. Open `PRESENTATION.md`
3. Click the preview button
4. Export to PDF/HTML/PPTX from the preview

### Option 5: Online Tools

- **Marp Web:** https://web.marp.app/
  - Upload `PRESENTATION.md`
  - Export to PDF/PPTX/HTML

- **Slidev:** https://sli.dev/
  - More advanced, Vue-based
  - Great for technical presentations

## Presentation Structure

The presentation includes:

1. **Title Slide** - Introduction
2. **Agenda** - Overview of topics
3. **The Challenge** - Problem statement
4. **What is Pipeline Builder?** - Solution overview
5. **Why Databricks + Pipeline Builder?** - Integration benefits
6. **Medallion Architecture** - Core concept
7. **Bronze/Silver/Gold Layers** - Detailed explanation
8. **Code Examples** - Real-world implementations
9. **Incremental Processing** - Advanced features
10. **Scheduling on Databricks** - Deployment patterns
11. **Real-World Example** - Complete pipeline
12. **Benefits & ROI** - Business value
13. **Key Takeaways** - Summary
14. **Q&A** - Closing slide
15. **Appendix** - Deep dives

## Customization

### Change Theme

Edit the `style:` section in the frontmatter:

```yaml
style: |
  section {
    background: linear-gradient(135deg, #your-color 0%, #your-color2 100%);
  }
  h1 {
    color: #your-accent-color;
  }
```

### Add Your Logo

Add to the header:
```yaml
header: '![logo](path/to/logo.png) Pipeline Builder on Databricks'
```

### Change Colors

The presentation uses:
- Primary: `#0066CC` (Databricks blue)
- Accent: `#004499` (Dark blue)
- Background: Gradient from `#f5f7fa` to `#e8ecf1`

## Tips

1. **For Live Presentations:** Use Marp preview or reveal-md for interactive navigation
2. **For Sharing:** Export to PDF for universal compatibility
3. **For Editing:** The markdown is clean and easy to modify
4. **For Printing:** PDF export works great for handouts

## File Size

- Markdown: ~25 KB (easy to edit)
- HTML: ~50-100 KB (when exported)
- PDF: ~500 KB - 2 MB (when exported)
- PowerPoint: ~1-3 MB (when exported)

## Next Steps

1. Review `PRESENTATION.md` and customize as needed
2. Choose your preferred tool (Marp recommended)
3. Export to your desired format
4. Practice your presentation!

## Troubleshooting

**Marp not working?**
- Make sure Node.js is installed
- Try: `npm install -g @marp-team/marp-cli --force`

**Pandoc errors?**
- Check that you have the required fonts installed
- For PDF: You may need LaTeX (TeX Live or MiKTeX)

**Styling issues?**
- Marp uses a subset of CSS - some advanced features may not work
- Stick to basic CSS properties for best compatibility



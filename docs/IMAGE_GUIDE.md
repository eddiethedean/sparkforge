# Image Guide for Presentation

This guide explains how to add images to your presentation slides.

## Current Implementation

The presentation generator currently uses:
- **Emoji icons** for simple visual elements (medals, clocks, etc.)
- **Font Awesome icons** (via CDN) for professional icons
- **Placeholder system** that can be replaced with actual images

## Image Sources

### Free Icon Resources

1. **Flaticon** (https://www.flaticon.com/)
   - Free with attribution
   - Search for: "databricks", "pipeline", "database", "analytics"
   - Download as PNG or SVG (512x512px minimum)

2. **Icons8** (https://icons8.com/)
   - Free tier available
   - Professional icons in multiple styles
   - Search for: "data", "cloud", "dashboard", "code"

3. **Font Awesome** (https://fontawesome.com/)
   - Already integrated via CDN
   - Free icons available
   - Currently used in HTML slideshow

4. **Unsplash** (https://unsplash.com/)
   - Free high-quality photos
   - Search for: "data", "technology", "business"

### Logo Sources

- **Databricks Logo**: https://www.databricks.com/press/media-kit
- **Company Logos**: Use official brand assets
- **SparkForge Logo**: Check project repository

## Image Requirements

- **Format**: PNG (transparent background) or SVG
- **Size**: Minimum 512x512px for quality
- **Resolution**: 300 DPI for print
- **Background**: Transparent (PNG) preferred

## Adding Images to Slides

### Option 1: Local Image Files

1. Create an `images/` directory in the project root
2. Place images with descriptive names:
   ```
   images/
   ├── databricks-logo.png
   ├── bronze-medal.png
   ├── silver-medal.png
   ├── gold-medal.png
   ├── pipeline-icon.png
   └── dashboard-icon.png
   ```

3. The generator will automatically detect and use these images if they match the image descriptions.

### Option 2: Replace Icons in Generated HTML

1. Open `presentation_slideshow.html`
2. Find the icon elements (look for `<i class="fas...">` or emoji)
3. Replace with `<img>` tags:
   ```html
   <img src="images/databricks-logo.png" alt="Databricks" style="width: 100px; height: auto;">
   ```

### Option 3: Add to PowerPoint Manually

1. Open the generated PowerPoint file
2. Insert → Pictures → From File
3. Position and resize as needed

## Image Mapping

The following images are referenced in the slides:

### Title Slide (Slide 1)
- Databricks logo (top right)
- Pipeline Builder/SparkForge logo (top left)
- Company logo (bottom center)

### Common Icons Needed
- 🥉 Bronze medal icon
- 🥈 Silver medal icon
- 🥇 Gold medal icon
- ⏰ Clock/time icons
- 💰 Dollar/cost icons
- ⚠️ Warning/alert icons
- ✅ Checklist icons
- 💻 Code icons
- 🗄️ Database/table icons
- 📊 Dashboard/chart icons
- ☁️ Cloud/Databricks icons
- 🛡️ Security/shield icons
- ⚡ Performance/speed icons

## Recommended Image Search Terms

- "data pipeline icon"
- "medallion architecture"
- "bronze silver gold medals"
- "databricks logo"
- "data analytics dashboard"
- "cloud computing icon"
- "database icon"
- "code programming icon"
- "security shield icon"
- "performance speed icon"

## Next Steps

1. Download images from recommended sources
2. Save to `images/` directory
3. Re-run the generator (it will detect local images)
4. Or manually add images to the generated files

## Notes

- All images should be properly licensed
- Attribute sources if required by license
- Keep file sizes reasonable (< 500KB per image)
- Use consistent style across all images


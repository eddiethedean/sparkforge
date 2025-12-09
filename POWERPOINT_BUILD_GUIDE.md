# PowerPoint Build Guide
## Step-by-Step Instructions for Creating the Presentation

This guide provides detailed instructions for building the PowerPoint presentation from the outline.

---

## Prerequisites Checklist

- [ ] Microsoft PowerPoint installed
- [ ] Access to stock image sources (Flaticon, Icons8, Unsplash)
- [ ] Outline document: `PRESENTATION_POWERPOINT_OUTLINE.md`
- [ ] Slide content document: `POWERPOINT_SLIDE_CONTENT.md` (will be created)
- [ ] Brand assets (logos, color scheme)

---

## Phase 1: Setup and Template Creation

### Step 1: Create PowerPoint Template

1. **Open Microsoft PowerPoint**
2. **Create New Presentation**
   - File → New → Blank Presentation

3. **Set Up Master Slide**
   - View → Slide Master
   - Design the master layout:
     - Header/Footer area
     - Title placeholder
     - Content area
     - Logo placement (top right or bottom)

4. **Define Color Scheme**
   - Design → Colors → Customize Colors
   - Create custom color scheme:
     - **Bronze:** #CD7F32 (RGB: 205, 127, 50)
     - **Silver:** #C0C0C0 (RGB: 192, 192, 192)
     - **Gold:** #FFD700 (RGB: 255, 215, 0)
     - **Accent Blue:** #0073E6 (Databricks blue)
     - **Text:** #000000 (Black)
     - **Background:** #FFFFFF (White)

5. **Set Default Fonts**
   - Home → Font → Set default:
     - **Title:** Arial Bold, 44pt
     - **Body:** Arial, 24pt
     - **Code:** Consolas or Courier New, 18pt

6. **Create Slide Layouts**
   - Title Slide
   - Content Slide (with bullet points)
   - Code Slide (with code block area)
   - Comparison Slide (two columns)
   - Diagram Slide (large content area)

7. **Save Template**
   - File → Save As → PowerPoint Template (.potx)
   - Name: `Pipeline_Builder_Template.potx`

---

## Phase 2: Asset Gathering

### Step 2: Gather Logos and Icons

**Logos to Download:**
- [ ] Databricks logo (PNG, transparent background)
- [ ] Pipeline Builder/SparkForge logo (if available)
- [ ] Company logo (if applicable)

**Icon Sources:**
- Flaticon.com (free with attribution)
- Icons8.com (free tier available)
- Font Awesome (web icons)

**Icons Needed:**
- [ ] Bronze medal icon
- [ ] Silver medal icon
- [ ] Gold medal icon
- [ ] Data flow arrows
- [ ] Database/table icons
- [ ] Clock/time icons
- [ ] Checkmark/success icons
- [ ] Warning/error icons
- [ ] Dashboard/chart icons
- [ ] Code/development icons
- [ ] Cloud/Databricks icons

**Download Specifications:**
- Format: PNG or SVG
- Size: At least 512x512px (for quality)
- Background: Transparent
- Color: Match presentation color scheme or use grayscale

---

## Phase 3: Building Slides

### General Slide Creation Process

For each slide:
1. Insert new slide with appropriate layout
2. Add title text
3. Add content (text, bullets, code)
4. Insert images/icons
5. Format and align
6. Apply animations (if desired)
7. Add speaker notes

### Slide-by-Slide Instructions

**Reference Documents:**
- `PRESENTATION_POWERPOINT_OUTLINE.md` - Slide structure and content
- `POWERPOINT_SLIDE_CONTENT.md` - Detailed content for each slide

**Quick Reference:**
- Use outline document for slide structure
- Use slide content document for exact text and code
- Follow image suggestions from outline

---

## Phase 4: Image and Diagram Creation

### Creating Diagrams

**Option 1: Use PowerPoint Shapes**
- Insert → Shapes
- Create diagrams using rectangles, arrows, circles
- Group shapes for easy manipulation

**Option 2: External Tools**
- Draw.io (diagrams.net) - Free, web-based
- Lucidchart - Professional diagrams
- Miro - Collaborative whiteboard

**Key Diagrams to Create:**
1. **Medallion Architecture Stack**
   - Three rectangles stacked
   - Labeled: Bronze, Silver, Gold
   - Color-coded

2. **Data Flow Diagram**
   - Arrows showing: Raw Data → Bronze → Silver → Gold
   - Include icons for each layer

3. **Incremental Processing Flow**
   - Timeline showing initial load vs incremental
   - Filter visualization

4. **Pipeline Execution Flow**
   - Steps in sequence
   - Dependencies shown

5. **Before/After Comparison**
   - Side-by-side code comparison
   - Metrics visualization

### Code Snippet Formatting

**Best Practices:**
1. Use monospace font (Consolas, Courier New)
2. Font size: 18-20pt (readable from back of room)
3. Use syntax highlighting if possible:
   - Insert → Text Box
   - Format → Font → Consolas
   - Manually color keywords (if needed)
4. Add line numbers (optional)
5. Use consistent indentation

**Alternative:**
- Take screenshots of code from IDE
- Insert as images
- Ensure high resolution

---

## Phase 5: Design Consistency

### Color Usage Guidelines

- **Bronze Layer:** Use bronze color (#CD7F32)
- **Silver Layer:** Use silver color (#C0C0C0)
- **Gold Layer:** Use gold color (#FFD700)
- **Accent:** Use Databricks blue for highlights
- **Text:** Black on white background
- **Code:** Dark background (optional) with light text

### Font Guidelines

- **Titles:** Arial Bold, 44pt
- **Subtitles:** Arial Bold, 32pt
- **Body Text:** Arial, 24pt
- **Code:** Consolas, 18-20pt
- **Captions:** Arial, 18pt

### Layout Guidelines

- **Margins:** Keep content within safe area
- **Alignment:** Use grid and guides
- **Spacing:** Consistent spacing between elements
- **Consistency:** Same layout for similar slide types

---

## Phase 6: Animations and Transitions

### Slide Transitions

- **Between Sections:** Use "Fade" or "Push"
- **Within Section:** Use "None" or "Fade"
- **Keep it subtle:** Avoid distracting transitions

### Object Animations

**Recommended Animations:**
- **Bullet Points:** "Fade In" or "Appear"
- **Diagrams:** "Wipe" or "Fade In"
- **Code Blocks:** "Fade In"
- **Images:** "Fade In"

**Timing:**
- Start: "On Click" or "After Previous"
- Duration: 0.5-1 second
- Delay: 0.1-0.3 seconds between items

---

## Phase 7: Quality Checklist

### Before Finalizing

- [ ] Spell check entire presentation (F7)
- [ ] Verify all code snippets are correct
- [ ] Check image quality and resolution
- [ ] Ensure all links work (if any)
- [ ] Test presentation on different screen sizes
- [ ] Verify color contrast (readable)
- [ ] Check font sizes (readable from back)
- [ ] Ensure consistent formatting
- [ ] Verify all slides have titles
- [ ] Check slide numbering

### Speaker Notes

For each slide, add speaker notes:
- Key talking points
- Timing reminders
- When to pause for questions
- Backup information for Q&A

**To Add Notes:**
- View → Notes Page
- Add notes in the notes pane

---

## Phase 8: Export and Backup

### Save Formats

1. **Editable Version:**
   - File → Save As → PowerPoint Presentation (.pptx)
   - Name: `Pipeline_Builder_Databricks_Presentation.pptx`

2. **PDF Backup:**
   - File → Export → Create PDF
   - Name: `Pipeline_Builder_Databricks_Presentation.pdf`
   - Useful for: Printing, sharing, backup

3. **Handout Version (Optional):**
   - File → Export → Create Handouts
   - 3 slides per page with notes

### File Organization

Create folder structure:
```
presentation/
├── Pipeline_Builder_Databricks_Presentation.pptx
├── Pipeline_Builder_Databricks_Presentation.pdf
├── assets/
│   ├── logos/
│   ├── icons/
│   ├── diagrams/
│   └── code_snippets/
├── PRESENTATION_POWERPOINT_OUTLINE.md
├── POWERPOINT_SLIDE_CONTENT.md
└── POWERPOINT_BUILD_GUIDE.md (this file)
```

---

## Time Estimates

- **Phase 1 (Setup):** 1-2 hours
- **Phase 2 (Assets):** 1-2 hours
- **Phase 3 (Slides):** 8-12 hours
- **Phase 4 (Images/Diagrams):** 4-6 hours
- **Phase 5 (Design):** 2-3 hours
- **Phase 6 (Animations):** 1-2 hours
- **Phase 7 (Quality Check):** 1-2 hours
- **Phase 8 (Export):** 30 minutes

**Total: 18-28 hours**

---

## Tips and Best Practices

1. **Work in Sections:** Complete one section at a time
2. **Save Frequently:** Use Ctrl+S often
3. **Use Master Slide:** Make global changes easily
4. **Test on Projector:** Preview on actual presentation screen
5. **Practice Timing:** Time yourself during creation
6. **Get Feedback:** Show to colleague before finalizing
7. **Backup Regularly:** Save multiple versions

---

## Troubleshooting

**Issue: Images look blurry**
- Solution: Use high-resolution images (at least 300 DPI)
- Re-insert images at full size

**Issue: Code is hard to read**
- Solution: Increase font size, use monospace font
- Consider using code screenshots instead

**Issue: Colors don't match**
- Solution: Use exact hex codes, create custom color palette
- Check color mode (RGB vs CMYK)

**Issue: File size too large**
- Solution: Compress images (Picture Tools → Compress Pictures)
- Remove unused slides or images

---

## Next Steps

1. Review this guide
2. Set up template (Phase 1)
3. Gather assets (Phase 2)
4. Start building slides (Phase 3)
5. Reference `POWERPOINT_SLIDE_CONTENT.md` for detailed content
6. Follow the outline structure
7. Complete quality checks
8. Export and backup

**Good luck building your presentation!**


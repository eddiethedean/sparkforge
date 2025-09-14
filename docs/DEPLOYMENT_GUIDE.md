# SparkForge Documentation Deployment Guide

This guide explains how to deploy the SparkForge documentation to Read the Docs.

## Prerequisites

1. **GitHub Repository**: Your SparkForge code must be in a GitHub repository
2. **Read the Docs Account**: Sign up at [readthedocs.org](https://readthedocs.org)
3. **Documentation Structure**: The docs are already set up in the `docs/` directory

## Deployment Steps

### 1. Connect Repository to Read the Docs

1. Go to [readthedocs.org](https://readthedocs.org) and sign in
2. Click "Import a Project"
3. Connect your GitHub account if not already connected
4. Select your SparkForge repository
5. Set the project name (e.g., `sparkforge`)
6. Set the repository URL to your GitHub repository

### 2. Configure Build Settings

Read the Docs will automatically detect the configuration from `.readthedocs.yml`:

```yaml
version: 2
sphinx:
  configuration: docs/conf.py
  fail_on_warning: false
python:
  version: 3.8
  install:
    - requirements: docs/requirements.txt
    - method: pip
      path: .
```

### 3. Trigger First Build

1. Click "Build version" on your project page
2. Monitor the build logs for any errors
3. The documentation will be available at: `https://sparkforge.readthedocs.io/`

### 4. Set Up Automatic Builds

1. Go to your project settings in Read the Docs
2. Under "Integrations", connect to GitHub webhooks
3. Enable "Build pull requests" if desired
4. Documentation will automatically rebuild when you push to the repository

## Configuration Files

### `.readthedocs.yml`
The main configuration file for Read the Docs:

```yaml
version: 2
sphinx:
  configuration: docs/conf.py
  fail_on_warning: false
python:
  version: 3.8
  install:
    - requirements: docs/requirements.txt
    - method: pip
      path: .
```

### `docs/conf.py`
Sphinx configuration with:
- Project information
- Extensions and themes
- Custom styling
- Build settings

### `docs/requirements.txt`
Documentation dependencies:
```
sphinx>=4.0.0
sphinx-rtd-theme>=1.0.0
myst-parser>=0.18.0
sphinx-copybutton>=0.5.0
sphinx-tabs>=3.4.0
sphinx-panels>=0.6.0
```

## Documentation Structure

```
docs/
├── conf.py              # Sphinx configuration
├── index.rst            # Main documentation page
├── quick_start_5_min.rst # 5-minute quick start
├── hello_world.rst      # Hello world example
├── getting_started.rst  # Getting started guide
├── examples/            # Examples documentation
├── notebooks/           # Notebooks documentation
├── _static/            # Static assets (CSS, images)
└── requirements.txt    # Documentation dependencies
```

## Troubleshooting

### Build Failures

1. **Check build logs** in Read the Docs dashboard
2. **Common issues**:
   - Missing dependencies in `requirements.txt`
   - Syntax errors in `.rst` files
   - Missing files referenced in documentation

### Local Testing

Test the documentation locally before deploying:

```bash
cd sparkforge
pip install -r docs/requirements.txt
cd docs
sphinx-build -b html . _build/html
```

### Updating Documentation

1. Make changes to documentation files
2. Test locally: `sphinx-build -b html . _build/html`
3. Commit and push to GitHub
4. Read the Docs will automatically rebuild

## Custom Domain (Optional)

To use a custom domain like `docs.sparkforge.com`:

1. Go to project settings in Read the Docs
2. Under "Domains", add your custom domain
3. Update DNS records as instructed
4. Enable HTTPS

## Version Management

Read the Docs automatically creates versions for:
- **Latest**: Latest commit on main branch
- **Stable**: Latest tagged release
- **All tags**: All Git tags in the repository

To create a new release version:
```bash
git tag v0.4.0
git push origin v0.4.0
```

## Advanced Configuration

### PDF Generation
The configuration already includes PDF generation:
```yaml
formats:
  - htmlzip
  - pdf
```

### Search Integration
Search is automatically enabled and will index your documentation.

### Analytics
Enable Google Analytics in Read the Docs project settings for usage statistics.

## Best Practices

1. **Test locally** before pushing changes
2. **Use meaningful commit messages** for documentation updates
3. **Keep documentation in sync** with code changes
4. **Regular updates** to keep content current
5. **Monitor build logs** for any issues

## Support

- **Read the Docs Documentation**: [docs.readthedocs.io](https://docs.readthedocs.io)
- **Sphinx Documentation**: [sphinx-doc.org](https://www.sphinx-doc.org)
- **GitHub Issues**: Report documentation issues in your repository

---

**🎉 Your SparkForge documentation will be live at `https://sparkforge.readthedocs.io/` once deployed!**

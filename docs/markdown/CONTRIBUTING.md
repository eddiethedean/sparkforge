# Contributing to SparkForge

Thank you for your interest in contributing to SparkForge! We welcome contributions from the community.

## 🚀 Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/yourusername/sparkforge.git
   cd sparkforge
   ```
3. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. **Install dependencies**:
   ```bash
   pip install -e ".[dev]"
   ```

## 🧪 Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=sparkforge --cov-report=html

# Run specific test categories
pytest -m "not slow"  # Skip slow tests
pytest -m "integration"  # Only integration tests
```

## 📝 Code Style

We use several tools to maintain code quality:

```bash
# Format code
black sparkforge/ tests/

# Sort imports
isort sparkforge/ tests/

# Lint code
flake8 sparkforge/ tests/

# Type checking
mypy sparkforge/
```

## 🔧 Development Setup

1. **Install pre-commit hooks**:
   ```bash
   pre-commit install
   ```

2. **Run the example**:
   ```bash
   python examples/basic_pipeline.py
   ```

## 📋 Pull Request Process

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** and add tests
3. **Run the test suite** to ensure everything passes
4. **Commit your changes**:
   ```bash
   git commit -m "Add: your feature description"
   ```

5. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request** on GitHub

## 🐛 Reporting Issues

When reporting issues, please include:

- Python version
- Spark version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Error messages/logs

## 💡 Feature Requests

We welcome feature requests! Please:

1. Check existing issues first
2. Provide a clear description
3. Explain the use case
4. Consider contributing the implementation

## 📚 Documentation

- Update docstrings for new functions/classes
- Add examples for new features
- Update README.md if needed
- Keep CHANGELOG.md updated

## 🏷️ Release Process

Releases are managed by maintainers:

1. Update version in `pyproject.toml` and `sparkforge/__init__.py`
2. Update CHANGELOG.md
3. Create a release tag
4. Publish to PyPI

## 🤝 Code of Conduct

Please be respectful and constructive in all interactions. We aim to create a welcoming environment for all contributors.

## 📞 Questions?

- Open a [GitHub Discussion](https://github.com/yourusername/sparkforge/discussions)
- Join our community chat
- Email: contributors@sparkforge.dev

Thank you for contributing to SparkForge! 🔥

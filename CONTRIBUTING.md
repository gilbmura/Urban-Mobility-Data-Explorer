# Contributing to Urban Mobility Data Explorer 🚀

Thank you for your interest in contributing to this project! This document provides guidelines for contributing to the Urban Mobility Data Explorer.

## Getting Started

### Prerequisites
- Python 3.8+
- MySQL/MariaDB
- Git

### Development Setup
```bash
# Fork and clone the repository
git clone https://github.com/yourusername/Urban-Mobility-Data-Explorer.git
cd Urban-Mobility-Data-Explorer

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Setup database (see README.md)
# Load test data
python scripts/simple_loader.py --csv data/processed/cleaned.csv

# Start development server
cd backend
python app.py
```

## How to Contribute

### 1. Fork the Repository
- Click the "Fork" button on GitHub
- Clone your fork locally

### 2. Create a Feature Branch
```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### 3. Make Your Changes
- Write clean, readable code
- Add comments for complex logic
- Follow existing code style
- Add tests for new features

### 4. Test Your Changes
```bash
# Run the test suite
python test_api.py

# Test data loading
python scripts/simple_loader.py --csv data/processed/cleaned.csv

# Test API endpoints
curl http://127.0.0.1:5000/health
curl http://127.0.0.1:5000/stats/summary
```

### 5. Commit Your Changes
```bash
git add .
git commit -m "Add: brief description of your changes"
```

### 6. Push and Create Pull Request
```bash
git push origin feature/your-feature-name
```

## Code Style Guidelines

### Python
- Follow PEP 8 style guide
- Use meaningful variable names
- Add docstrings to functions and classes
- Keep functions small and focused

### Example:
```python
def calculate_tip_percentage(fare_amount: float, tip_amount: float) -> float:
    """
    Calculate tip percentage based on fare and tip amounts.
    
    Args:
        fare_amount: The total fare amount
        tip_amount: The tip amount given
        
    Returns:
        Tip percentage as a decimal (0.0 to 1.0)
    """
    if fare_amount <= 0:
        return 0.0
    return tip_amount / fare_amount
```

### API Endpoints
- Use RESTful conventions
- Include proper error handling
- Add input validation
- Document parameters

### Example:
```python
@app.get('/api/v1/trips')
def get_trips(limit: int = 50, offset: int = 0):
    """
    Get paginated list of trips.
    
    Args:
        limit: Maximum number of trips to return (default: 50)
        offset: Number of trips to skip (default: 0)
        
    Returns:
        JSON list of trip objects
    """
    # Implementation here
```

## Types of Contributions

### 🐛 Bug Fixes
- Fix existing bugs
- Improve error handling
- Add input validation

### ✨ New Features
- New API endpoints
- Data analysis algorithms
- Performance improvements
- UI enhancements

### 📚 Documentation
- Improve README
- Add code comments
- Create tutorials
- Update API documentation

### 🧪 Testing
- Add unit tests
- Integration tests
- Performance tests
- Security tests

## Pull Request Guidelines

### Before Submitting
- [ ] Code follows style guidelines
- [ ] All tests pass
- [ ] Documentation updated
- [ ] No merge conflicts
- [ ] Descriptive commit messages

### PR Template
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Other (please describe)

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Screenshots (if applicable)
Add screenshots for UI changes

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes (or documented)
```

## Issue Guidelines

### Bug Reports
When reporting bugs, include:
- Clear description of the issue
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Python version, etc.)
- Screenshots if applicable

### Feature Requests
When requesting features, include:
- Clear description of the feature
- Use case and benefits
- Mockups or examples if applicable
- Implementation suggestions

## Development Workflow

### 1. Planning
- Check existing issues
- Discuss major changes in issues first
- Plan your approach

### 2. Development
- Create feature branch
- Write code with tests
- Test thoroughly
- Update documentation

### 3. Review
- Self-review your code
- Request review from maintainers
- Address feedback
- Update tests if needed

### 4. Merge
- Squash commits if needed
- Write clear merge message
- Delete feature branch after merge

## Code Review Process

### For Contributors
- Be responsive to feedback
- Ask questions if unclear
- Be open to suggestions
- Learn from reviews

### For Reviewers
- Be constructive and helpful
- Explain reasoning for suggestions
- Be respectful and professional
- Focus on code, not person

## Release Process

### Version Numbering
We use Semantic Versioning (MAJOR.MINOR.PATCH):
- MAJOR: Breaking changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes (backward compatible)

### Release Checklist
- [ ] All tests pass
- [ ] Documentation updated
- [ ] Version bumped
- [ ] Changelog updated
- [ ] Release notes written

## Community Guidelines

### Be Respectful
- Use welcoming and inclusive language
- Be respectful of differing viewpoints
- Accept constructive criticism gracefully
- Focus on what's best for the community

### Be Professional
- Keep discussions on-topic
- Use clear and concise language
- Provide helpful feedback
- Follow the project's code of conduct

## Getting Help

### Resources
- README.md - Setup and usage
- DEPLOYMENT.md - Deployment guide
- Issues - Bug reports and feature requests
- Discussions - General questions and ideas

### Contact
- Create an issue for bugs or features
- Use discussions for questions
- Tag maintainers for urgent issues

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

---

Thank you for contributing to Urban Mobility Data Explorer! 🎉

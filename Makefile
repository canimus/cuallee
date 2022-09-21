black: # Format code
	@black cuallee
	@black test

clean: # Remove workspace files
	@find . -name "__pycache__" -exec rm -rf {} + 
	@rm -rf ./.pytest_cache
	@rm -rf ./htmlcov
	@rm -rf dist/
	@rm -rf cuallee/cuallee.egg-info/
	@rm -rf cuallee.egg-info/
	@rm -rf build/
	@python -c "print('Cleaning: 👌')"

cov: # Run test and coverage
	coverage run -m pytest tests/unit
	coverage xml -o temp/coverage.xml

flake: # Lint code
	@flake8 --ignore=E501,W503,E731,E722 --max-cognitive-complexity=30 cuallee
	@python -c "print('Linting: 👌')"

report: # Launches the coverage report
	@coverage html
	@python -m http.server --directory htmlcov

build: # Package wheel
	@python -m build
#sort: # Sort import
#	@isort src

type: # Verify static types
	@mypy --install-types --non-interactive cuallee
	@python -c "print('Types: 👌')"
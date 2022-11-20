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
	@rm -rf spark-warehouse
	@python -c "print('Cleaning: 👌')"

cov: # Run test and coverage
	coverage run -m pytest test/unit
	coverage xml -o temp/coverage.xml

flake: # Lint code
	@flake8 --ignore=E501,W503,E731,E722 --max-cognitive-complexity=30 cuallee
	@python -c "print('Linting: 👌')"

report: # Launches the coverage report
	@coverage html
	@python -m http.server --directory htmlcov

pack: # Package wheel
	@python -m build

type: # Verify static types
	@mypy --install-types --non-interactive cuallee
	@python -c "print('Types: 👌')"

unit: # Run unit test
	@pytest test/unit

twine: # Upload to python index
	@twine upload dist/*

testers: # Generate all test functions on folder/
	@for i in `ls -1 test/unit/pyspark_dataframe/*.py | cut -d"/" -f4`; do touch "$(folder)/$$i"; done
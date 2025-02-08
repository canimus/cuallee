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
	@rm -rf __blobstorage__
	@rm -rf .mypy_cache
	@rm -rf .coverage
	@rm -rf .DS_Store
	@rm -rf test.data
	@rm -rf coverage.xml

	@rm -rf spark-warehouse
	@python -c "print('Cleaning: 👌')"

cov: # Run test and coverage
	@pytest test/unit
	@coverage report

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

joss: # produce pdf
	@docker run --rm --volume $$PWD/paper:/data --user $$(id -u):$$(id -g) --env JOURNAL=joss openjournals/inara

testers: # Generate all test functions on folder/
	@mkdir -p "$(folder)"
	@echo "Created: $(folder)"
	@for i in `ls -1 test/unit/pyspark_dataframe/*.py | cut -d"/" -f4`; do touch "$(folder)/$$i"; done
	@python -c "print('To Test: 🏃')"

loadenv:
	@echo "for i in $`cat .env$`; do export $$i; done"

inventory:
	@python -c "from cuallee.core.utils import inventory; inventory()"

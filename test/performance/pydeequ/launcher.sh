echo "Running Performance Tests for Framework: PYDEEQU"
docker run -it --rm -v $PWD:/usr/src --name cuallee-tester-pydeequ cuallee-tester-pydeequ python test_performance_pydeequ.py
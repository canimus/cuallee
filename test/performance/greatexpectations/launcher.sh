echo "Running Performance Tests for Framework: GREAT_EXPECTATIONS"
docker run -it -v $PWD:/usr/src --rm --name cuallee-tester-gx --rm cuallee-tester-gx python test_performance_gx.py
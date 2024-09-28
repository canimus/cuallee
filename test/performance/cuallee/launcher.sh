echo "Running Performance Tests for Framework: CUALLEE"
docker run -it -v $PWD:/usr/src --rm --name cuallee-tester-cuallee --rm cuallee-tester-cuallee python test_performance_cuallee.py

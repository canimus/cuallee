echo "Running Performance Tests for Framework: SODA"
docker run --env-file .secrets -it --rm -v $PWD:/usr/src --name cuallee-tester-soda cuallee-tester-soda python test_performance_soda.py


# Alternative without .secret file
#docker run -e SODA_KEY=REPLACE_WITH_YOUR_SODA_KEY -e SODA_SECRET=REPLACE_WITH_YOUR_SODA_SECRET -it --rm -v $PWD:/usr/src --name cuallee-tester-soda cuallee-tester-soda python test_performance_soda.py


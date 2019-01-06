#   This script isn't used in CI / CD ; it is there for manual testing if needed

#	Build docker container
sudo docker build -t vplauzon/cosbak .

#	Publish image
sudo docker push vplauzon/cosbak

#	Test image
sudo docker run --name test-cosbak -d vplauzon/cosbak
sudo docker run --name test-cosbak -it vplauzon/cosbak bash
#  Clean up after test
sudo docker stop test-cosbak && sudo docker container prune -f
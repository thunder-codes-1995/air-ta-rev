# MSD v2 API (backend)
After cloning the repository:

```bash 
pip3 install -r requirements.txt  
python3 index.py
```
## Configuration
Configuration settings (such as DB details, port) are stored in `.env` file    
Don't hardcode anything in the source files please



## Running using docker
#### Build image
`docker build . -t msdv2 
`

#### Run image
`docker run msdv2:latest 
`

##AWS Autodeployment
Application is configured to automatically deploy in AWS Development environment.  
This is done using github actions.
Each push to `develop` branch will trigger automated deployment in AWS  
It uses docker to spin up instance in AWS so if you are seeing any issues after you push your code, make sure it is running in docker

After push, it will take up to 1 minute to deploy and application will be available under http://msdv2-api-dev.eu-central-1.elasticbeanstalk.com/

It is using `.env` configuration